# EIP-8141 (Frame Transactions) — Python execution-specs implementation notes

Cross-check reference for the Besu implementation. Researched 2026-08-31.

**Sources**
- Branch: `ethereum/execution-specs` @ `eips/amsterdam/eip-8141`, head commit `6798542ebd017b683b688489d770bf206c8bd3ba` (2026-08-22). All file paths below are under `src/ethereum/forks/amsterdam/` unless stated.
- EIP text: `ethereum/EIPs` master `EIPS/eip-8141.md` (Draft, created 2026-01-29). Requires 1559, 2718, 2780, 3529, 3607, 4844, 7594, 7623, 7702, 7708, 7778, 7825, 8037.
- Key modules on the branch:
  - `transactions/frame_transaction.py` (861 lines: dataclasses, static validity, sig validation, intrinsic gas)
  - `frame_processing.py` (333: admission + tx-level processing)
  - `vm/frame_interpreter.py` (723: per-frame loop, atomic batch, default code)
  - `vm/__init__.py` (FrameContext, attempt_approval, ENTRY_POINT)
  - `vm/instructions/frame.py` (APPROVE + introspection opcodes)
  - `vm/gas.py` (constants, GasMeter, state gas, settlement)
  - `blocks.py` (FrameReceipt / FrameTransactionReceipt), `fork.py` (dispatch, apply_fork), `vm/instructions/storage.py`/`system.py` (state gas refills)
- Pending fix: **PR #3443 (open)** changes block execution-gas accounting to pre-refund (see Deviations).

---

## 1. Transaction type: dataclasses, RLP, signing hash

### 1.1 Dataclasses (exact fields/types)

`FrameMode(UintEnum, boundary=STRICT)`: `DEFAULT=0, VERIFY=1, SENDER=2`.
`FrameFlag(UintFlag, boundary=STRICT)`: `APPROVE_PAYMENT=1, APPROVE_EXECUTION=2, ATOMIC_BATCH=4`; `APPROVE_SCOPE_MASK = APPROVE_PAYMENT|APPROVE_EXECUTION` (=3).
`FrameStatus(UintEnum)`: `FAILURE=0, SUCCESS=1, SKIPPED=2`.
`FrameSignatureScheme(UintEnum, boundary=STRICT)`: `ARBITRARY=0, SECP256K1=1, P256=2`.

STRICT boundary means an undefined mode/flag-bit/scheme **fails RLP decode** — no separate validity check exists for these.

```python
class GasLimits:            # "limits"
    execution: U64
    state: U64

class Frame:
    mode: FrameMode
    flags: FrameFlag
    to: Bytes0 | Address    # empty bytes == None target
    gas_limits: GasLimits
    value: U256
    data: Bytes

class FrameSignature:
    scheme: FrameSignatureScheme
    signer: Bytes           # 0 or 20 bytes (checked at validation, not decode)
    message: Bytes0 | Bytes32   # decode enforces exactly 0 or 32 bytes
    signature: Bytes

class TransactionFees:      # "fees"
    max_priority_fee_per_gas: Uint
    max_fee_per_gas: Uint
    max_fee_per_blob_gas: U256

class FrameTransaction:
    chain_id: U64
    nonce: U256
    sender: Address
    frames: Tuple[Frame, ...]
    signatures: Tuple[FrameSignature, ...]
    fees: TransactionFees
    blob_versioned_hashes: Tuple[VersionedHash, ...]
```

Constants: `MAX_FRAMES_PER_TX = 64`; `EXPIRY_VERIFIER = 0x...8141` (20-byte address `0x0000000000000000000000000000000000008141`); `EXPIRY_DATA_LENGTH = 8`; `EXPIRY_VERIFIER_CODE = 0x60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd`; `FRAME_ENTRY_POINT = 0x00000000000000000000000000000000000000aa` (in `vm/__init__.py`).

### 1.2 RLP wire format

`encode_transaction`: `b"\x06" + rlp.encode(tx)`; decode: `rlp.decode_to(FrameTransaction, tx[1:])` when first byte == 6. `ethereum_rlp` encodes dataclasses as lists of their fields in declaration order; `UintEnum`/`UintFlag` subclass `Uint`, so they encode as minimal big-endian scalars. So:

```
0x06 || rlp([
  chain_id, nonce, sender,
  [[mode, flags, to, [execution, state], value, data], ...],   # frames
  [[scheme, signer, message, signature], ...],                 # signatures
  [max_priority_fee_per_gas, max_fee_per_gas, max_fee_per_blob_gas],
  [blob_versioned_hash, ...]
])
```

- `to` (named `target` in the EIP): **empty byte string (`0x80`) for None**; otherwise exactly 20 bytes. Empty `to` resolves to `tx.sender` in every mode (`resolve_frame_target`).
- `limits` and `fees` are **nested lists**, as is each frame and each signature entry.
- Decode-time rejections (types): mode>2, any flag bit ≥8, scheme>2, `to` len ∉ {0,20}, `message` len ∉ {0,32}, `execution`/`state` > 2^64-1 (U64), `chain_id` > 2^64-1 (U64).
- Tx hash: `keccak256(0x06 || rlp(tx))` over the full encoding (signatures included).

### 1.3 Signing hash (`compute_frame_signature_hash`)

Only the raw `signature` bytes of entries with **empty `message`** are elided (replaced by empty bytes); entries with an explicit 32-byte digest keep their bytes. Nothing else is elided — chain_id, nonce, sender, frames, fees, blob hashes, and each signature's `scheme`/`signer`/`message` all remain.

```python
def compute_frame_signature_hash(tx: FrameTransaction) -> Hash32:
    elided_signatures = []
    for signature in tx.signatures:
        if len(signature.message) == 0:
            elided_signatures.append(replace(signature, signature=Bytes(b"")))
        else:
            elided_signatures.append(signature)
    elided_tx = replace(tx, signatures=tuple(elided_signatures))
    return keccak256(b"\x06" + rlp.encode(elided_tx))
```

### 1.4 Signature validation (`validate_signature`)

Runs during static validation for **every** entry, before any frame executes. Message resolution: empty → canonical sig hash; 32 bytes → explicit digest, but **all-zero digest is invalid** (reserves 0 as SIGPARAM's "canonical" marker). `signer` must be 0 or 20 bytes; empty resolves to `tx.sender`.

- `SECP256K1`: signature must be exactly 65 bytes = `v(1) || r(32) || s(32)`; `v ∈ {0,1}`; `0 < r < SECP256K1N`; `0 < s <= SECP256K1N // 2` (low-s mandatory); recovered address must equal resolved signer. Returns the resolved signer `Address`.
- `P256`: exactly 128 bytes = `r || s || qx || qy`; `0 < r < SECP256R1N`; `0 < s <= SECP256R1N // 2` (low-s); **`resolved_signer == keccak256(qx||qy)[12:]` checked before the curve verification**; then `secp256r1_verify(r,s,qx,qy,msg)` must pass. Returns resolved signer.
- `ARBITRARY`: signer must be empty; bytes are not validated; **resolved signer is `None`**.

Note: signature validation is outside the EVM — ecrecover/P256VERIFY precompiles must NOT appear in the block-level access list for this (pinned by `test_bal_omits_signature_validation_precompiles`).

---

## 2. Static validity + intrinsic gas (`validate_frame_transaction`)

Order of checks (each raises a typed `InvalidTransaction` subclass):

1. `tx.nonce >= U64.MAX_VALUE (2^64-1)` → `NonceOverflowError` (so max acceptable nonce = 2^64-2; test `test_nonce_at_maximum` pins 2^64-2 valid → increments to 2^64-1).
2. `max_fee_per_gas > U256.MAX` / `max_priority_fee_per_gas > U256.MAX` → `FeeOverflowError` (fields are `Uint`, unbounded at decode).
3. `max_fee_per_gas < max_priority_fee_per_gas` → `PriorityFeeGreaterThanMaxFeeError`.
4. Blobs: `blob_count == 0 and max_fee_per_blob_gas != 0` → `InvalidMaxFeePerBlobGasError`; `blob_count > BLOB_COUNT_LIMIT (6)` → `BlobCountExceededError`; every hash must start with `0x01` → `InvalidBlobVersionedHashError`. (A frame tx is *not required* to carry blobs, unlike 4844.)
5. `1 <= len(frames) <= 64` → `FrameCountError`.
6. Compute sig hash; validate all signatures (`InvalidSignatureError` / `InvalidFrameError`), collecting `resolved_signers` tuple.
7. Per-frame loop (accumulating `total_frame_execution_gas`, `total_frame_state_gas`, `total_frame_gas`), all `InvalidFrameError`:
   - running `total_frame_gas (exec+state) > 2^64-1` → invalid;
   - `mode != SENDER and value != 0` → invalid ("only sender frames can transfer value");
   - `APPROVE_EXECUTION` flag with explicit `to != sender` → invalid;
   - `ATOMIC_BATCH` flag: frame must not be VERIFY; must not be last frame; next frame must not be VERIFY;
   - **in-batch frames (flag set on it OR on its predecessor) must have zero approval-scope flags** — including the terminating frame;
   - expiry-verifier frame (`mode==VERIFY and to==EXPIRY_VERIFIER`): at most one per tx; `flags == 0`; `value == 0`; `gas_limits.state == 0`; `len(data) == 8`.
8. Intrinsic gas (below); then EIP-7825 cap:

```python
execution_gas_cap_usage = max(
    Uint(intrinsic.execution) + total_frame_execution_gas,
    Uint(intrinsic.calldata_floor),
)
if execution_gas_cap_usage > TX_MAX_GAS_LIMIT:   # 16,777,216
    raise TransactionGasLimitExceededError(...)
```

**State gas is exempt from the 7825 cap** (bounded only by U64 encoding + block state-gas capacity).

Derived anchors returned in `FrameTransactionValidation`:
- `standard_gas_limit = intrinsic.execution + Σ(exec limits) + Σ(state limits)` — settlement anchor.
- `max_gas = max(standard_gas_limit, intrinsic.calldata_floor + Σ(state limits))` — inclusion/max-cost anchor (this is `tx_env.gas_limit`).

### Intrinsic gas (`calculate_frame_transaction_intrinsic_cost`)

Constants (`vm/gas.py GasCosts`): `TX_FRAME_INTRINSIC = 12000`, `TX_PER_FRAME = 475`, `FRAME_SIGNATURE_SCHEME_SECP256K1 = 2800`, `_P256 = 6700`, `_ARBITRARY = 100`, `TX_VALUE_COST = 6000`, `TX_DATA_TOKEN_STANDARD = 4`, `TX_DATA_TOKEN_FLOOR = 16`.

```python
tokens, data_length, value_transfer_gas = 0, 0, 0
for frame in tx.frames:
    tokens += count_tokens_in_data(frame.data)        # zero byte=1, nonzero=4
    data_length += len(frame.data)
    if frame.value > 0 and isinstance(frame.to, Address) and frame.to != tx.sender:
        value_transfer_gas += GasCosts.TX_VALUE_COST   # 6000
signature_gas = 0
for signature in tx.signatures:
    signature_gas += signature_verification_gas(signature)
    for data in (signature.signer, signature.message, signature.signature):
        tokens += count_tokens_in_data(data)
        data_length += len(data)
floor_tokens = data_length * 4                       # every byte uniform (EIP-7976)
base_execution_gas = 12000 + len(frames)*475 + signature_gas + value_transfer_gas
execution      = base_execution_gas + tokens * 4
calldata_floor = base_execution_gas + floor_tokens * 16
```

Notes:
- Charged-as-calldata bytes = every frame's `data` plus every signature's `signer`, `message`, `signature` bytes. For elided-at-signing entries, the *actual* signature bytes still count toward intrinsic gas.
- `TX_VALUE_COST` applies only for value-bearing frames with an **explicit** target that is **not** the sender (a value frame with empty `to` or `to == sender` pays nothing extra).
- No recipient-access component in intrinsic gas: target access is charged at frame entry from the frame's own execution budget.
- The floor's base includes base + per-frame + signature verification + value-transfer costs (so floor never undercuts the intrinsic base). Floor bytes are counted uniformly at 16/byte (4 floor-tokens/byte × 4 gas/token), per EIP-7976.

---

## 3. Transaction processing

### 3.1 Dispatch and chain-id (fork.py `process_transaction`)

- Tx goes into transactions trie (`0x06 || rlp`), then `chain_id(tx) != block chain id` → `WrongChainIdError` (frame txs included — chain-id check happens **before** frame dispatch), then `isinstance(tx, FrameTransaction)` → `process_frame_transaction(block_env, block_output, tx, index)` (separate flow from `check_transaction`/`process_top_level`).
- `apply_fork` installs `EXPIRY_VERIFIER_CODE` at `0x...8141` at fork activation, preserving any existing nonce/balance.

### 3.2 Admission (`check_frame_transaction`) — exact order

```python
validation = validate_frame_transaction(tx)                  # 1. static validity (see §2)
tx_state = TransactionState(parent=block_env.state)
execution_gas_grant = Σ frame.gas_limits.execution
state_reservation   = Σ frame.gas_limits.state
execution_reservation = max(intrinsic.execution + execution_gas_grant,
                            intrinsic.calldata_floor)
check_block_gas_capacity(block_env, block_output,            # 2. block capacity
    execution_reservation, state_reservation, calculate_total_blob_gas(tx))
sender_account = get_account(tx_state, tx.sender)
effective_gas_price = calculate_effective_gas_price(tx, base_fee)   # 3. max_fee >= base_fee
check_max_fee_per_blob_gas(tx.blob_versioned_hashes,          # 4. blob fee cap (only if blobs)
    tx.fees.max_fee_per_blob_gas, block_env.excess_blob_gas)
check_nonce(tx, sender_account.nonce)                         # 5. nonce == sender nonce (exact)
max_cost = validation.max_gas * tx.fees.max_fee_per_gas \
         + total_blob_gas * blob_gas_price(excess_blob_gas)
if max_cost > U256.MAX: raise MaxCostOverflowError            # 6. max cost fits U256
```

Key differences from the regular flow: **no sender recovery** (sender is a field, authenticated by signature entries), **no EIP-3607 EOA check** (sender may be a contract), **no upfront balance check**, **no upfront gas purchase**, **no sender nonce increment here** (all deferred to `APPROVE`). No init-code-size check (no top-level create).

Block capacity (`check_block_gas_capacity`): per-dimension exact reservations —
`execution_reservation <= block_gas_limit - block_gas_used`; `state_reservation <= block_gas_limit - block_state_gas_used`; `tx_blob_gas <= MAX_BLOB_GAS_PER_BLOCK - blob_gas_used` (blob max = 21 blobs × 2^17). (Regular txs instead reserve `min(TX_MAX_GAS_LIMIT, tx.gas)` execution and the whole `tx.gas` in state.)

The `TransactionEnvironment` gets: `origin=tx.sender` (rebound per frame later), `gas_limit=max_gas`, `execution_gas_grant=Σexec`, `state_gas_reservoir=0` (**reservoir model unused**), `calldata_floor`, empty access lists, `accounts_with_paid_writes={tx.sender}`, `top_level_context=None`, and a `FrameContext`:

```python
frame_context = vm.FrameContext(
    tx=tx,
    signature_hash=validation.signature_hash,
    resolved_signers=validation.resolved_signers,   # None for ARBITRARY
    standard_gas_limit=validation.standard_gas_limit,
    max_cost=max_cost,
    current_frame_index=Uint(0),
    frame_receipts=[],
    payer=None,
    sender_approved=False,
    state_gas_left=StateGas(Uint(0)),
    outstanding_charge_owners={},                    # (address, key) -> frame index
)
```

### 3.3 `process_frame_transaction` — top level

```python
tx_env = check_frame_transaction(block_env, block_output, tx, index)
tx_output = process_frames(block_env, tx_env)         # raises FrameTransactionExecutionError
payer = frame_context.payer;  assert payer is not None
settlement = settle_frame_transaction_gas(
    frame_context.standard_gas_limit,
    tx_env.calldata_floor,
    Uint(tx_output.gas_left) + Uint(tx_output.state_gas_left),  # tx_unused_gas (both dims)
    tx_output.refund_counter,
    StateGas(Uint(tx_output.state_gas_used)))
gas_used = settlement.execution_gas_used + settlement.state_gas_used   # (changes under PR #3443)
disburse_frame_gas_fees(block_env, tx_env, gas_used)
block_output.block_gas_used        += settlement.execution_gas_used
block_output.block_state_gas_used  += settlement.state_gas_used
block_output.blob_gas_used         += calculate_total_blob_gas(tx)
block_output.cumulative_gas_used   += gas_used
receipt = make_frame_receipt(tx, payer, block_output.cumulative_gas_used,
                             tuple(frame_context.frame_receipts))
trie_set(block_output.receipts_trie, rlp.encode(index), receipt)
block_output.block_logs += tx_output.logs
for address in tx_output.accounts_to_delete:
    clear_account_preserving_balance(tx_env.state, address)
incorporate_tx_into_block(tx_env.state, block_env.block_access_list_builder)
```

If `process_frames` raises (`VERIFY` frame failed / SENDER before approval / no payer), the whole tx is invalid (block invalid; in t8n the tx is rejected) — the `TransactionState` overlay is simply not incorporated.

### 3.4 Per-frame loop (`process_frames`)

Shared journal across frames:

```python
journal = FrameJournal(
    warm_addresses={tx.sender},   # sender pre-warmed (EIP-2929/3651 seed)
    warm_storage_keys=set(),
    refund_counter=0,
    accounts_to_delete=set())
```

Loop per frame, in order:
1. `frame_context.current_frame_index = index`.
2. Atomic batch open: if frame has `ATOMIC_BATCH` flag and no batch open, snapshot `AtomicBatch(first_frame_index, copy_tx_state(state), copy_frame_context(tx_env), copy_frame_journal(journal))`.
3. If skipping (previous batch frame failed): append `FrameReceipt(SKIPPED, GasUsed(0,0), logs=())` and continue; when the unflagged (terminating) frame is reached, close the batch and stop skipping. Skipped frames do NOT clear transient storage, do NOT rebind origin, and are exempt from the SENDER-approval check.
4. `if frame.mode == SENDER and not sender_approved: raise FrameTransactionExecutionError("SENDER frame before execution approval")` — invalidates the whole tx.
5. `tx_state.transient_storage.clear()` — transient storage discarded between frames.
6. Rebind origin: `tx_env.origin = tx.sender` for SENDER frames, else `FRAME_ENTRY_POINT` (0xaa). ORIGIN returns this at every depth; ENTRY_POINT is **not** pre-warmed.
7. Seed the frame's state pool: `frame_context.state_gas_left = frame.gas_limits.state`.
8. `outcome = execute_frame(...)` (below). If `mode == VERIFY and status == FAILURE`: raise `FrameTransactionExecutionError("VERIFY frame failed")`.
9. `incorporate_frame_outcome(journal, outcome)` (refunds + accounts_to_delete; warm sets were committed inside execute_frame on success only); append receipt.
10. Batch bookkeeping: if the frame failed and a batch is open → `journal = unroll_atomic_batch(tx_env, open_batch)`; if the failing frame terminates the batch, close it, else set `skip_batch=True`. If it succeeded and terminates the batch, close it (consecutive batches supported).

After the loop: `if frame_context.payer is None: raise FrameTransactionExecutionError("no frame approved gas payment")`. Logs = concatenation of receipt logs in frame order (unrolled receipts contribute none — their logs were emptied).

Settlement inputs are derived **from the final receipts** (not from live pools):

```python
for frame, receipt in zip(tx.frames, frame_context.frame_receipts, strict=True):
    unused_execution_gas += frame.gas_limits.execution - receipt.gas_used.execution
    unused_state_gas     += frame.gas_limits.state     - receipt.gas_used.state
    state_gas_used       += receipt.gas_used.state
return TransactionOutput(gas_left=unused_execution_gas, refund_counter=journal.refund_counter,
    logs=logs, accounts_to_delete=journal.accounts_to_delete, error=None, return_data=b"",
    state_gas_left=unused_state_gas, state_gas_used=int(state_gas_used))
```

So skipped frames' full budgets, and any state gas removed from receipts by refills/rollbacks, automatically count as unused.

### 3.5 Single frame execution (`execute_frame`)

Order of operations:

1. `resolved_target = frame.to or tx.sender`. Take `entry_snapshot = copy_frame_context(tx_env)` (state pool/receipts/ownership rollback point).
2. Fresh meter: `GasMeter(gas_left=frame.gas_limits.execution, reservoir=None)` — **no reservoir inside frame txs**.
3. Seed the frame's access sets: `journal.warm_addresses ∪ {coinbase} ∪ precompiles`, `journal.warm_storage_keys`. (There is no tx access list.)
4. **Frame-entry access charge**: warm (100) if resolved target in the set, else cold (3000) and add to set, charged from the frame's own execution budget. OOG here → receipt `(FAILURE, GasUsed(execution=whole budget, state=0), logs=())`; nothing to restore.
5. **Default code branch**: if `mode == VERIFY and resolved_target not in PRECOMPILES and target code hash == EMPTY_CODE_HASH` → `execute_default_verify_code` (see §3.7); no EVM is built. On its `ExceptionalHalt` (APPROVE state charge unaffordable): restore entry snapshot, receipt `(FAILURE, execution=whole budget, state=0)`. On return: warm sets committed only on SUCCESS; receipt gas: `execution = budget - gas_left` (i.e. just the entry access), `state = budget - state_gas_left` (sender-creation charge, if any). NOTE: default code applies **only in VERIFY mode**; a precompile target dispatches normally in *every* mode; an EIP-7702-delegated target has a non-empty code hash and therefore runs delegated code, never default code. DEFAULT/SENDER frames with a codeless target run an ordinary call to empty code (immediate success).
6. **Value balance check** (`frame.value != 0`, so only SENDER frames): `get_account(state, tx_env.origin).balance < value` → receipt `(FAILURE, execution=consumed so far, state=0)` — a revert-style failure that keeps unspent gas, not a full-budget halt.
7. `create_evm_from_frame`: charges, in order, (a) `NEW_ACCOUNT` **state** gas if `value > 0` and target not alive; (b) EIP-7702 delegation resolution access (warm/cold **execution** gas via `resolve_delegated_code_address`, adds designated address to warm set, disables precompiles for the delegated code). Any `ExceptionalHalt` → restore entry snapshot; receipt `(FAILURE, execution=whole budget, state=0)`. The EVM is built with `caller=tx_env.origin`, `current_target=resolved_target`, `value`, `call_data=frame.data`, `should_transfer_value=True`, **`is_static = (mode == VERIFY)`**, `depth=0`.
8. `process_call(evm)` — the ordinary interpreter. It snapshots `copy_tx_state` + `copy_frame_context` at entry; moves value (emitting the EIP-7708 transfer log when caller != target and amount > 0); on `ExceptionalHalt` does `restore_state_gas` (no-op on meter for frame txs) + `forfeit_remaining_gas` (gas_left = 0); on `Revert` keeps gas_left; on any error restores both tx state and frame context.
9. If `evm.error`: `restore_frame_context(tx_env, entry_snapshot)` — extends the state-gas rollback over the frame-entry charges.
10. Receipt: `GasUsed(execution = budget - gas_meter.gas_left, state = budget - frame_context.state_gas_left)`; SUCCESS → commit `evm.accessed_addresses/storage_keys` into the journal (this is what keeps a payer warm for later frames), logs = `evm.logs`, accounts_to_delete carried; FAILURE → logs empty, accesses discarded, `state` is 0 by construction (context restored to entry).

Refund counter: returned via the outcome from `gas_meter.refund_counter` (0 for failed frames since `restore_state_gas` zeroes it) and accumulated in the journal only after VERIFY-failure would have aborted.

### 3.6 Atomic batches

Batch = maximal run `[i..j]` where frames `i..j-1` carry `ATOMIC_BATCH` and frame `j` does not. Static validity guarantees: no VERIFY frames in a batch, batch cannot dangle past the last frame, and **no approval scopes anywhere in the batch (terminator included)** — so unrolls can never move `payer`/`sender_approved` (asserted in `unroll_atomic_batch`).

```python
def unroll_atomic_batch(tx_env, batch) -> FrameJournal:
    assert frame_context.payer == batch.context_snapshot.payer
    assert frame_context.sender_approved == batch.context_snapshot.sender_approved
    executed_batch_receipts = frame_context.frame_receipts[int(batch.first_frame_index):]
    restore_tx_state(tx_env.state, batch.state_snapshot)
    restore_frame_context(tx_env, batch.context_snapshot)
    for receipt in executed_batch_receipts:
        frame_context.frame_receipts.append(
            replace(receipt,
                    gas_used=replace(receipt.gas_used, state=Uint(0)),
                    logs=()))
    return batch.journal
```

Effects: state restored to pre-batch; frame-context restore also undoes **refills the batch's frames applied to pre-batch receipts** and the ownership map; executed batch frames keep their `status` and `gas_used.execution` but get `state=0` and empty logs; the batch's journal copy resumes (warm sets, refunds, deletions from batch frames dropped). Execution gas the batch consumed **stays charged**; its state gas is refunded via the zeroed receipts. Remaining batch frames after the failure get `SKIPPED` receipts with 0/0 gas.

### 3.7 Default code (native, not EVM)

`execute_default_verify_code` is evaluated **natively** — no EVM/bytecode. It runs only for VERIFY frames whose resolved target has the empty code hash and is not a precompile. It draws **no execution gas** (frame's only execution charge is the entry access); it can draw **state gas** through APPROVE's sender-creation charge.

```python
allowed_scope = frame.flags & APPROVE_SCOPE_MASK
if not allowed_scope:                          return FAILURE   # reverts frame => tx invalid
signature_index = 0 if APPROVE_EXECUTION in allowed_scope else 1
if len(tx.signatures) <= signature_index:      return FAILURE
signature = tx.signatures[signature_index]
if signature.scheme != SECP256K1:              return FAILURE
if len(signature.message) != 0:                return FAILURE   # must sign canonical hash
if resolved_signers[signature_index] != resolved_target: return FAILURE
if not attempt_approval(tx_env, allowed_scope): return FAILURE
return SUCCESS
```

So: signature index 0 authorizes frames allowed to approve execution (scope 2 or 3); index 1 authorizes payment-only frames (scope 1). Any FAILURE of a VERIFY frame invalidates the tx. For SENDER/DEFAULT modes with codeless targets, the EIP's "return successfully as if calling empty code" is realized by the normal empty-code call path.

### 3.8 Expiry verifier

Executed as a **real EVM call** to the installed runtime code at `0x8141` (no native shortcut in the spec, though the EIP allows equivalent direct evaluation). The code reverts unless calldata is exactly 8 bytes and `timestamp <= expiry` (`expiry >= timestamp` succeeds, strict `>` reverts). Since it is a VERIFY frame, a revert invalidates the whole transaction. Static validity already forces flags=0/value=0/state=0/len(data)=8 and at most one such frame.

### 3.9 `APPROVE` / `attempt_approval`

Opcode `0xAA`, stack `[offset, length, scope]` (offset on top). Gas: memory expansion only (`ZERO + extend_memory.cost`), like RETURN. Implementation:

```python
def approve(evm):
    offset = pop(); length = pop(); scope = pop()
    extend_memory = calculate_gas_extend_memory(evm.memory, [(offset, length)])
    charge_gas(evm, GasCosts.ZERO + extend_memory.cost)
    frame_context = frame_transaction_context(evm)   # halt if not a frame tx
    frame = tx.frames[current_frame_index]; resolved_target = resolve_frame_target(tx, frame)
    evm.memory += b"\x00" * extend_memory.expand_by
    if evm.current_target != resolved_target: raise Revert     # ADDRESS check (DELEGATECALL keeps ADDRESS => allowed)
    if scope & ~U256(APPROVE_SCOPE_MASK) != 0: raise Revert    # bits beyond mask never allowed
    if not attempt_approval(evm.tx_env, FrameFlag(Uint(scope))): raise Revert
    evm.output = memory_read_bytes(evm.memory, offset, length)  # RETURN semantics
    evm.running = False                                         # terminates the CALL FRAME successfully
```

- A refused approval **reverts only the requesting call frame**, not the whole frame (pinned by `test_refused_approval_reverts_only_its_call_frame`).
- On success it terminates the current call frame like `RETURN` (with return data); it does not terminate the whole frame unless executed at depth 0.
- It performs **no `is_static` check**: its writes deliberately bypass the VERIFY static restriction (and any STATICCALL context) — the only mutation allowed there.
- Scope semantics in `attempt_approval` (vm/__init__.py):

```python
allowed_scope = frame.flags & APPROVE_SCOPE_MASK
if not scope or scope & ~allowed_scope: return False   # empty scope or beyond frame flags
if approves_execution:
    if frame_context.sender_approved: return False      # already approved
    if resolved_target != tx.sender: return False       # only sender approves execution
if approves_payment:
    if frame_context.payer is not None: return False    # already set
    if not (frame_context.sender_approved or approves_execution): return False  # exec first
    payer_balance = get_account(state, resolved_target).balance
    if Uint(payer_balance) < frame_context.max_cost: return False
if approves_execution:
    frame_context.sender_approved = True
if approves_payment:
    if not is_account_alive(state, tx.sender):
        charge_frame_state_gas(frame_context, StateGasCosts.NEW_ACCOUNT)  # may OOG-halt
    increment_nonce(state, tx.sender)
    set_account_balance(state, resolved_target, payer_balance - max_cost)  # escrow max_cost
    frame_context.payer = resolved_target
return True
```

- **Payer collection happens here**: the full `max_cost` (= `max_gas * max_fee_per_gas + blob_gas * blob_gas_price`) is deducted from the resolved target's balance as escrow, and the **sender's nonce is incremented** (regardless of who pays).
- If incrementing the nonce would create the sender account (dead sender), `NEW_ACCOUNT` state gas (120 × 1530 = 183,600) is charged from the executing frame's pool *before* the increment; OOG → `ExceptionalHalt` of the current call frame with **no approval effects** (rollback discards even an execution approval this call just recorded, via the frame-context snapshot mechanism).
- Approval effects are ordinary transaction-state writes + frame-context fields; they roll back together on any enclosing revert (a frame that APPROVEs via a nested call and then reverts loses the payer, the escrow, and the nonce bump — leaving no payer → tx invalid; pinned by `test_frame_revert_discards_the_approval_it_granted`).
- The payer needs no explicit warming: it is the frame's resolved target, already charged/warmed at frame entry (kept warm for later frames via the journal on frame success).
- `set_account_balance` writes an absolute balance (i.e. debits the escrow immediately). The EIP-7708 transfer log is **not** emitted for the escrow/refund/fee settlement moves (they use `set_account_balance`/`create_ether`, not `move_ether`).

### 3.10 Gas settlement + fee disbursement

`settle_frame_transaction_gas(standard_gas_limit, calldata_floor, tx_unused_gas, refund_counter, tx_state_gas)` (branch head):

```python
gas_used_before_refund = standard_gas_limit - tx_unused_gas
applied_refund = min(Uint(refund_counter), gas_used_before_refund // 5)   # EIP-3529 cap
gas_used_after_refund = gas_used_before_refund - applied_refund
execution_gas_used = max(int(gas_used_after_refund) - int(tx_state_gas),  # plain ints:
                         int(calldata_floor))                              # can go negative
# state_gas_used = tx_state_gas
```

- `tx_unused_gas` = `TransactionOutput.gas_left + state_gas_left` = Σ over frames of (exec budget − receipt exec used) + (state budget − receipt state used), so it includes skipped-frame budgets and refill-reduced state.
- State-gas refills bypass the refund cap by design (they already reduced receipts and hence `gas_used_before_refund`); storage refunds (EIP-3529) accumulate across successful frames only and are capped at 1/5 of pre-refund usage.
- Calldata floor binds the **execution dimension alone**: a floor-bound state-growing tx pays `floor + state` in full (differs from EIP-8037's transaction-total floor for legacy envelopes).
- The subtraction `gas_used_after_refund - tx_state_gas` is computed in plain ints before the floor clamps it (state-dominated tx's refund can push it negative).
- **Branch head**: payer-facing `gas_used = execution_gas_used + state_gas_used`, and the same `execution_gas_used` (post-refund) is added to `block_gas_used`. **PR #3443 (open) changes this**: block execution gas becomes pre-refund `max(gas_used_before_refund - tx_state_gas, calldata_floor)` per EIP-7778, while the payer/receipt stay post-refund; `FrameTransactionGasSettlement` gains a `gas_used` field. The current EIP text already specifies the #3443 behaviour.

Fee disbursement (`disburse_frame_gas_fees`):

```python
blob_gas_fee = total_blob_gas * blob_gas_price(excess_blob_gas)
charged_fee  = gas_used * effective_gas_price + blob_gas_fee
payer_refund = max_cost - charged_fee            # escrow minus charge; blob fee cancels
priority_fee_per_gas = effective_gas_price - base_fee_per_gas
transaction_fee = gas_used * priority_fee_per_gas
create_ether(state, payer, payer_refund)         # balance += (no transfer log)
create_ether(state, coinbase, transaction_fee)
```

The refund is escrow-based (max fee priced) — refunding "unused gas at effective price" would under-refund. Coinbase gets `gas_used * priority_fee`; base-fee portion is burned implicitly (escrowed, never re-credited).

Block accounting contributions: `block_gas_used += execution_gas_used` (post-refund at head; pre-refund under #3443); `block_state_gas_used += state_gas_used` (net after refills; per-frame receipts sum); `blob_gas_used += 2^17 * len(blob_versioned_hashes)`; `cumulative_gas_used += gas_used` (payer-facing, used in the receipt). Header validity: `max(block_gas_used, block_state_gas_used) == header.gas_used` (EIP-8037).

Blob handling: `max_fee_per_blob_gas` used only for the inclusion check (`>= blob_gas_price`, only when blobs are present); payer pays `blob_gas * blob_base_fee` inside `max_cost`/`charged_fee` (both terms include it so the refund cancels it); blobs optional; per-tx blob limit 6; `BLOBHASH` works in every frame.

### 3.11 Receipt

`blocks.py`:

```python
class GasUsed:                  # nested list [execution, state]
    execution: Uint             # not reduced by refunds; final at frame completion
    state: Uint                 # live until tx end (refills/rollbacks lower it)

class FrameReceipt:             # [status, gas_used, logs]
    status: FrameStatus         # 0 fail, 1 success, 2 skipped
    gas_used: GasUsed
    logs: Tuple[Log, ...]       # emptied when the frame's batch unrolled

class FrameTransactionReceipt:  # [cumulative_gas_used, payer, frame_receipts]
    cumulative_gas_used: Uint   # payer-facing (post-refund) cumulative sum
    payer: Address
    frame_receipts: Tuple[FrameReceipt, ...]
```

Encoded into the receipts trie as `0x06 || rlp([cumulative_gas_used, payer, [[status, [execution, state], [logs...]], ...]])`. **No tx-level status field and no bloom field.** The tx's logs (for block bloom / indexing) are the concatenation of frame-receipt logs in frame order. Exactly one receipt per frame (skipped frames get `SKIPPED, [0,0], []`). Deposit-request parsing (`requests.py`) flattens frame-receipt logs the same way.

Self-destructed accounts (`accounts_to_delete`, EIP-6780 same-tx-created only) are processed after all frames via `clear_account_preserving_balance` (nonce→0, code cleared, storage destroyed, **balance kept**).

---

## 4. Interpreter / opcode details

Opcode numbers: `APPROVE=0xAA`, `TXPARAM=0xB0`, `FRAMEDATALOAD=0xB1`, `FRAMEDATACOPY=0xB2`, `FRAMEPARAM=0xB3`, `SIGPARAM=0xB4`, `SIGDATACOPY=0xB5`.

All six introspection ops + APPROVE call `frame_transaction_context(evm)` first: if `tx_env.frame_context is None` (any non-frame tx, including system calls) → `InvalidParameter` (an `ExceptionalHalt` subclass) → exceptional halt consuming all gas in the call frame. They are available at **every call depth** within a frame tx (including inside CREATE init code and STATICCALL children).

Threading: the tx context lives on `TransactionEnvironment.frame_context` (mutable `FrameContext`, §3.2), reachable from every `Evm` via `evm.tx_env`. `current_frame_index` is advanced by the frame loop; `frame_receipts` grow as frames complete; `state_gas_left` is the executing frame's live pool.

### TXPARAM (gas 2 = BASE); stack: pops `param`, pushes value

| param | value (python) |
|---|---|
| 0x00 | `U256(0x06)` (tx type) |
| 0x01 | `U256(tx.nonce)` |
| 0x02 | `U256.from_be_bytes(tx.sender)` — 20-byte address as zero-extended uint |
| 0x03 | `max_priority_fee_per_gas` |
| 0x04 | `max_fee_per_gas` |
| 0x05 | `max_fee_per_blob_gas` |
| 0x06 | `U256(frame_context.max_cost)` — **exactly** `max_gas * max_fee_per_gas + total_blob_gas * blob_gas_price(block excess_blob_gas)`; fixed at admission; < 2^256 guaranteed by admission check |
| 0x07 | `len(blob_versioned_hashes)` |
| 0x08 | `U256.from_be_bytes(signature_hash)` — the canonical sig hash as uint |
| 0x09 | `len(frames)` |
| 0x0A | `current_frame_index` |
| 0x0B | `len(signatures)` |
| 0x0C | `U256(state_gas_left)` — live remaining pool of the executing frame (mid-frame value, any depth) |
| other | `InvalidParameter` → exceptional halt |

### FRAMEPARAM (gas 2); pops `frame_index` (top) then `param`; out-of-bounds index → halt

| param | value |
|---|---|
| 0x00 | `resolved_target` as uint (empty `to` already resolved to sender) |
| 0x01 | `limits.execution` |
| 0x02 | `mode` |
| 0x03 | `flags` (full field, incl. batch bit) |
| 0x04 | `len(data)` |
| 0x05 | receipt `status` — **halts if `frame_index >= current_frame_index`** (current or future) |
| 0x06 | `flags & APPROVE_SCOPE_MASK` |
| 0x07 | 1 if `ATOMIC_BATCH` in flags else 0 |
| 0x08 | `value` |
| 0x09 | `limits.state` |
| 0x0A | receipt `gas_used.execution` — halts if current/future |
| 0x0B | receipt `gas_used.state` — halts if current/future; **live value**: may have been lowered by a later refill, restored by a rollback, zeroed by a batch unroll |
| other | halt |

Statuses/gas are read from the receipts list; completed batch-unrolled frames keep their status (SUCCESS/FAILURE) with zeroed state gas; frames skipped by a failed batch read `SKIPPED (2)` with 0/0.

### SIGPARAM (gas 2); pops `signature_index` (top) then `param`; OOB → halt

| param | value |
|---|---|
| 0x00 | `resolved_signer` as uint — **halts for ARBITRARY entries** (`resolved_signers[i] is None`); for k1/p256, the 20-byte resolved signer (explicit signer, or `tx.sender` if the field was empty) |
| 0x01 | `scheme` |
| 0x02 | `msg`: 0 if the message field is empty (canonical hash marker); else the explicit 32-byte digest as uint (all-zero digest was statically invalid, so 0 is unambiguous) |
| 0x03 | `len(signature)` — **only for ARBITRARY**; halts for protocol-validated schemes |
| other | halt |

### SIGDATACOPY (CALLDATACOPY gas: 3 + 3/word + memory expansion); pops `memOffset, dataOffset, length, signatureIndex`
OOB index → halt; **non-ARBITRARY scheme → halt** (raw bytes of protocol-validated schemes are never introspectable). Zero-padded reads past the end.

### FRAMEDATALOAD (gas 3): pops `offset` (top), `frame_index`; OOB index → halt; CALLDATALOAD semantics on the chosen frame's `data` (zero-padded).
### FRAMEDATACOPY (CALLDATACOPY gas): pops `memOffset, dataOffset, length, frameIndex`; OOB index → halt; note the **gas (incl. memory expansion) is charged before the index bounds check** in the python code (same pattern for SIGDATACOPY; for FRAMEDATALOAD/FRAMEPARAM/SIGPARAM/TXPARAM the constant gas is also charged first).

### Static context
VERIFY frames run with `is_static=True` from depth 0; children inherit (`params.is_staticcall or evm.is_static`). Ordinary write ops raise `WriteInStaticContext`; `APPROVE` is the sole exception (no static check; its writes go through state helpers directly). Value: statically forced to 0 in non-SENDER frames, so the CALL-with-value static restriction is never hit at the frame top level.

---

## 5. State gas (EIP-8037) inside frame transactions

Two coexisting models, selected by `evm.tx_env.frame_context`:

- **Reservoir model** (all non-frame txs): `GasMeter.reservoir: StateGasReservoir` with `state_gas_left / state_gas_baseline / state_gas_spilled / state_gas_committed_spill`; state charges spill into `gas_left` when the reservoir empties; children receive the whole reservoir (no 63/64), refunds credit LIFO (spill first).
- **Frame model**: every meter in a frame tx has `reservoir=None`. State gas lives on `FrameContext.state_gas_left`, a **frame-scoped pool shared by every call depth** — never forwarded, split, or 63/64'd. Junction:

```python
def charge_state_gas(evm, amount):
    frame_context = evm.tx_env.frame_context
    if frame_context is None:
        charge_state_gas_from_meter(evm.gas_meter, amount)   # reservoir + spill
    else:
        charge_frame_state_gas(frame_context, amount)        # pool only

def charge_frame_state_gas(frame_context, amount):
    if frame_context.state_gas_left < amount:
        raise OutOfGasError          # exceptional halt of the CURRENT call frame
    frame_context.state_gas_left -= amount
```

**Execution gas can never fund state charges inside a frame tx** (no spill). A short pool halts only the current call frame (ordinary OOG semantics); tx validity is unaffected unless it fails a VERIFY frame.

Charge points (unchanged from 8037): `SSTORE` fresh slot set = `STORAGE_SET` (64 × 1530 = 97,920); new-account creation = `NEW_ACCOUNT` (120 × 1530 = 183,600) at `CALL*` value-to-dead-account, `CREATE*`, `SELFDESTRUCT` sweep-to-dead-beneficiary, frame-level value transfer to dead target, and APPROVE sender creation; code deposit = `len(code) × 1530` at deposit time. Execution gas is charged **before** state gas at each opcode so an execution OOG doesn't consume state gas.

**Attribution & ownership** (`outstanding_charge_owners: Dict[(Address, Bytes32), Uint]`): only SSTORE charges record an owner (the charging frame's index) —

```python
# sstore(): after charge_gas + charge_state_gas
if frame_context is not None and state_gas != 0:
    frame_context.outstanding_charge_owners[(evm.current_target, key)] = \
        frame_context.current_frame_index
```

**Refill** (slot set then restored to original zero, i.e. `current != new and original == new and original == 0`):

```python
owner = frame_context.outstanding_charge_owners.pop((evm.current_target, key))
credit_frame_state_gas_refund(frame_context, owner, StateGasCosts.STORAGE_SET)

def credit_frame_state_gas_refund(frame_context, owner, amount):
    if owner == frame_context.current_frame_index:
        frame_context.state_gas_left += amount        # back into the live pool
        assert state_gas_left <= frame.gas_limits.state   # never exceeds own budget
    else:
        receipt = frame_context.frame_receipts[int(owner)]
        frame_context.frame_receipts[int(owner)] = replace(receipt,
            gas_used=replace(receipt.gas_used,
                             state=receipt.gas_used.state - Uint(amount)))
```

Same-frame refill returns spendable pool; **cross-frame refill edits the owner frame's receipt in place** (payer made whole at settlement via `unused_state_gas`; the executing frame gains no budget). Account-creation charges (CALL*/CREATE preflight) are refunded to `current_frame_index` when the child fails or is never entered — those never cross frames.

**Journaling**: `copy_frame_context` / `restore_frame_context` are taken alongside **every** `copy_tx_state`/`restore_tx_state` (in `process_call`, `process_create`, frame entry, batch open). The snapshot copies `current_frame_index`, `frame_receipts` (shallow list — entries are frozen and replaced wholesale), `payer`, `sender_approved`, `state_gas_left`, `outstanding_charge_owners` (dict copy). Reverting a call restores the pool, receipt edits, and ownership map to the checkpoint; reverting a frame restores to frame entry (receipt state = 0); a batch unroll restores to batch entry (undoing refills to pre-batch receipts). Refund counter (`gas_meter.refund_counter`, execution-side EIP-3529) is separately zeroed for failed frames by `restore_state_gas`.

**Settlement of the state dimension** is receipt-driven, not pool-driven: `state_gas_used = Σ receipt.gas_used.state`; the block's state counter takes exactly that (net after refills, "clamping" unnecessary since receipts never go negative).

`SELFDESTRUCT` never refills state gas (even for same-tx-created contracts). `test_state_gas.py` pins: same-frame refill, cross-frame refill, refill after intermediate modification, exhaustion, APPROVE sender creation (+unaffordable), batch-unroll and frame-revert restoring refilled receipts.

---

## 6. Deviations / notable gaps between branch and EIP text

1. **Block execution gas pre- vs post-refund (real deviation, fix pending).** Branch head adds *post-refund* execution gas to `block_gas_used`; the EIP (current text, commit `7d1c8bf...`) mandates *pre-refund* (`block_execution_gas = max(gas_used_before_refund - tx_state_gas, calldata_floor_gas)`, EIP-7778). Open PR ethereum/execution-specs#3443 fixes the branch and adds a payer-facing `gas_used` to `FrameTransactionGasSettlement`. Besu should implement the EIP/#3443 behaviour: receipt cumulative + payer fee post-refund; block execution counter pre-refund; state dimension net; floor applied independently to both.
2. **chain_id width.** EIP constraint says `tx.chain_id < 2**256`; python decodes it as `U64` (decode failure beyond 2^64-1). Chain-id equality with the block's (U64) chain id is checked in `process_transaction` for all tx types, so the practical difference is only the error classification for absurd chain ids.
3. **Nonce bound.** EIP says `tx.nonce < 2**64`; python rejects `nonce >= 2^64-1` (max valid 2^64-2, matching EIP-2681 handling of other types and the `test_nonce_at_maximum` test).
4. **Signature `message` length.** EIP `validate_signature` returns false for other lengths; python can never see them (decode enforces `Bytes0 | Bytes32`), though a defensive check exists.
5. **Expiry verifier is executed via EVM** in the spec (the EIP allows native evaluation as an optimization; gas must match real execution).
6. **Default code is evaluated natively** (no bytecode); consistent with the EIP's protocol-defined semantics; gas observable = entry access only (+ possible APPROVE state charge).
7. **`GasLimits.execution` bound**: EIP constraints list only `limits.state <= 2^64-1` explicitly, but the python `U64` type bounds both fields at decode (the running `total_frame_gas <= 2^64-1` check subsumes it anyway).
8. **Static-validity error granularity**: python raises distinct exception types (FrameCountError, InvalidFrameError, FeeOverflowError, MaxCostOverflowError, TransactionGasLimitExceededError, ...) — t8n/test exception mapping (`TYPE_6_*` exceptions in the testing package) relies on these categories.
9. Mempool rules (MAX_VERIFY_GAS=100k, MAX_VERIFY_STATE_GAS=500k, validation prefixes, banned opcodes, canonical paymaster) are EIP-only — nothing in the state-transition spec implements them.
10. `EXPIRY_VERIFIER` install at fork activation preserves pre-existing nonce/balance and only sets code.

Minor implementation facts worth mirroring exactly:
- Admission order: static validity → block capacity → effective-gas-price (max_fee ≥ base_fee) → blob-fee cap → **nonce** → max-cost overflow.
- `max_cost` uses **max_gas** (not standard_gas_limit) and the *current block's* blob gas price.
- Warm-set seed per frame: journal ∪ {coinbase} ∪ precompiles; sender is warm from tx start via the journal; failed frames contribute no warmth; ENTRY_POINT not pre-warmed.
- Frame-entry target access is charged in every mode/outcome (precompile, default code, delegated, dead); 7702 designation resolution charges a second (warm/cold) access from the same frame budget.
- Value balance check reads the **origin's** balance (sender; only SENDER frames can carry value) and produces a revert-style FAILURE (gas so far), whereas entry-charge/preparation failures forfeit the whole execution budget.
- `cumulative_gas_used` in the frame receipt is the payer-facing post-refund figure.

---

## 7. Test inventory (what will be tested)

Tracker: execution-specs discussion #3358 ("Test Cases Tracker for EIP-8141"), maintained by @gurukamath. Tests live in `tests/amsterdam/eip8141_frame_transactions/` on the branch and fill under the **Bogota pseudo-fork** in the testing framework (`packages/testing/.../forks/eips/bogota/eip_8141.py`; type-6 tx support, `TX_FRAME_INTRINSIC=12000`, `TX_PER_FRAME=475`). Merged via PRs #3047 (initial), #3393 (approval context across batches/reverts), #3396 (impl update), #3398 (BAL: sig-validation precompiles absent), #3412 (frame-entry access charge), #3418 (CI); staging PRs also flow through SamWilsn/eth1.0-specs (#13 target resolution + BAL, #14 blob-fee-without-blobs). Open: #3443 (refund/7778 settlement fix + `test_storage_refund_settlement` orderings).

By module/category:
- **Core flows** (`test_frame_transactions.py`): `test_transfer_with_default_code`, `test_contract_sender_approves`, `test_eoa_paymaster`, `test_atomic_batch_rollback` (unrolls_executed_frames / skips_remaining_frames), `test_sender_frame_before_approval`, `test_verify_frame_reverts`, `test_frame_revert_discards_the_approval_it_granted`, `test_refused_approval_reverts_only_its_call_frame`.
- **Expiry verifier** (`test_expiry_verifier.py`): `test_expiry_verifier_frame` (future / at-timestamp / expired).
- **Introspection** (`test_introspection.py`): `test_txparam` (+ `_max_cost`, `_sender_and_sig_hash`, `_state_gas_left`), `test_frameparam` (+ `_resolved_target`, `_atomic_batch_set`, `_gas_used`, `_halts`), `test_introspection_halts` (non-frame tx), `test_framedataload`, `test_framedatacopy`, `test_sigparam` (+ `_resolved_signer`), `test_sigdatacopy`.
- **Warmth** (`test_warmth.py`): `test_sender_is_warm`, `test_coinbase_is_warm`, `test_frame_target_entry_charge`, `test_warmth_carry_to_next_frame`, `test_warmth_from_inner_call`, `test_payer_warm_after_default_code_payment_approval`, `test_origin_warmth_by_frame_mode`.
- **State gas** (`test_state_gas.py`): `test_same_frame_refill`, `test_cross_frame_refill`, `test_refill_after_intermediate_modification`, `test_state_gas_exhaustion`, `test_approve_creates_sender`, `test_approve_sender_creation_unaffordable`, `test_batch_unroll_restores_refilled_receipt`, `test_frame_revert_restores_refilled_receipt`.
- **Gas settlement** (`test_gas_settlement.py`): `test_calldata_floor_with_state_gas`, `test_storage_refund_settlement` (extended by #3443 to pin floor vs pre/post-refund orderings).
- **Value transfer** (`test_value_transfer.py`): `test_value_transfer`, `test_value_transfer_exceeding_balance`.
- **Static validity** (`test_static_validity.py`): `test_invalid_tx_fields(_transaction)`, `test_frame_constraints(_transaction)`, `test_expiry_verifier_constraints(_transaction)`, `test_signature_constraints(_transaction)`, `test_gas_limit_cap_from_frame_gas(_transaction)`, `test_gas_limit_cap_from_calldata_floor(_transaction)`, `test_gas_limit_cap_exempts_state_gas(_transaction)`; planned (🚧): `test_raw_rlp_decode_rejections`.
- **Admission validity** (`test_admission_validity.py`): `test_admission_constraints`, `test_nonce_at_maximum`, `test_block_capacity_reservations` (execution/state, at/above limit).
- **Block access lists** (`test_block_access_lists.py`): `test_bal_atomic_batch_write`, `test_bal_atomic_batch_skipped_frame_absent`, `test_bal_frame_revert_write_dropped`, `test_bal_unaffordable_designation_absent`, `test_bal_sponsored_payer_and_sender`, `test_bal_omits_signature_validation_precompiles`.
- **Target resolution / gas boundaries** (`test_target_resolution.py`): `test_precompile_target`, `test_precompile_target_rejecting_its_input`, `test_verify_frame_precompile_rejecting_its_input`, `test_verify_frame_precompile_target`*, `test_verify_frame_target_above/inside_the_precompile_gap`, `test_delegated_target_entry_charge`, `test_dead_target_entry_charge`, `test_delegated_to_precompile_target`, `test_verify_frame_delegated_to_precompile_target`, `test_frame_entry_gas_boundary`, `test_entry_charge_halt_unrolls_batch`, `test_default_code_entry_gas_shortfall`. (*tracker name; module also splits the rejecting-input VERIFY case.)
