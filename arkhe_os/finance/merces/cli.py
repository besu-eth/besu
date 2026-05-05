#!/usr/bin/env python3
"""
CLI para Merces: arkhe-merces.
Comandos: deposit, withdraw, transfer, status
"""

import argparse

def cmd_deposit(args):
    print(f"Depositando {args.amount} tokens...")

def cmd_withdraw(args):
    print(f"Levantando {args.amount} tokens...")

def cmd_transfer(args):
    print(f"Transferindo {args.amount} tokens para {args.receiver}...")

def cmd_status(args):
    print("Saldo confidencial: [oculto]")

def main():
    parser = argparse.ArgumentParser(description="ARKHE Merces CLI")
    subparsers = parser.add_subparsers(dest="command")

    p_dep = subparsers.add_parser("deposit")
    p_dep.add_argument("amount", type=int)
    p_wdr = subparsers.add_parser("withdraw")
    p_wdr.add_argument("amount", type=int)
    p_trf = subparsers.add_parser("transfer")
    p_trf.add_argument("receiver", type=str)
    p_trf.add_argument("amount", type=int)
    subparsers.add_parser("status")

    args = parser.parse_args()
    {"deposit": cmd_deposit, "withdraw": cmd_withdraw, "transfer": cmd_transfer, "status": cmd_status}[args.command](args)

if __name__ == "__main__":
    main()
