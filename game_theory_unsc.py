# game_theory_unsc.py
import numpy as np
import matplotlib.pyplot as plt

class UNReformGame:
    def __init__(self):
        # Estratégias: 0 = Cooperar (aceitar reforma), 1 = Bloquear (vetar)
        self.payoffs = {
            ('P5_RU', 'Cooperar'): {'self': 30, 'other_P5': 40, 'global': 60},
            ('P5_RU', 'Bloquear'): {'self': 50, 'other_P5': 20, 'global': 30},
            ('P5_CN', 'Cooperar'): {'self': 35, 'other_P5': 35, 'global': 55},
            ('P5_CN', 'Bloquear'): {'self': 45, 'other_P5': 25, 'global': 40},
            ('G4_UA', 'Cooperar'): {'self': 80, 'other_P5': 50, 'global': 90},
            ('G4_UA', 'Bloquear'): {'self': 10, 'other_P5': 10, 'global': 20}
        }
        self.lambda2_global = 0.59
        self.history = []

    def nash_equilibrium(self):
        """Calcula equilíbrio de Nash com payoffs assimétricos"""
        # Simplificação: dois jogadores representativos (P5_RU vs G4_UA)
        # Matriz de payoff (Cooperar, Bloquear)
        payoff_RU = [[30, 50], [40, 30]]  # RU: C, B vs G4: C, B
        payoff_G4 = [[80, 10], [70, 20]]

        # Melhor resposta para RU
        br_RU = []
        for g4_action in [0, 1]:
            best = max([payoff_RU[ru][g4_action] for ru in [0,1]])
            br_RU.append([ru for ru in [0,1] if payoff_RU[ru][g4_action] == best])

        # Melhor resposta para G4
        br_G4 = []
        for ru_action in [0, 1]:
            best = max([payoff_G4[g4][ru_action] for g4 in [0,1]])
            br_G4.append([g4 for g4 in [0,1] if payoff_G4[g4][ru_action] == best])

        # Interseção: equilíbrio de Nash
        nash = []
        for ru in [0,1]:
            for g4 in [0,1]:
                if ru in br_RU[g4] and g4 in br_G4[ru]:
                    nash.append((ru, g4))
        return nash

    def replicator_dynamics(self, steps=100):
        """Evolução temporal das estratégias (dinâmica de replicador)"""
        # População: 193 Estados
        x_RU = 0.7  # probabilidade inicial de RU cooperar
        x_G4 = 0.4  # probabilidade inicial de G4 cooperar
        history_RU = [x_RU]
        history_G4 = [x_G4]

        for _ in range(steps):
            # Payoff esperado
            payoff_RU_coop = 30 * x_G4 + 40 * (1 - x_G4)
            payoff_RU_block = 50 * x_G4 + 30 * (1 - x_G4)
            avg_RU = x_RU * payoff_RU_coop + (1 - x_RU) * payoff_RU_block
            dx_RU = x_RU * (payoff_RU_coop - avg_RU)

            payoff_G4_coop = 80 * x_RU + 10 * (1 - x_RU)
            payoff_G4_block = 70 * x_RU + 20 * (1 - x_RU)
            avg_G4 = x_G4 * payoff_G4_coop + (1 - x_G4) * payoff_G4_block
            dx_G4 = x_G4 * (payoff_G4_coop - avg_G4)

            x_RU += dx_RU * 0.1
            x_G4 += dx_G4 * 0.1
            x_RU = np.clip(x_RU, 0, 1)
            x_G4 = np.clip(x_G4, 0, 1)
            history_RU.append(x_RU)
            history_G4.append(x_G4)

        return history_RU, history_G4

    def simulate_blockage_scenario(self):
        """Cenário: Rússia veta, Uniting for Peace ativado"""
        # 1. Veto russo
        self.lambda2_global = 0.55
        print(f"[SIM] Veto russo aplicado. λ₂ cai para {self.lambda2_global}")

        # 2. Ativação do circuit breaker (λ₂ < 0.60 por 3 meses)
        if self.lambda2_global < 0.60:
            print("[SIM] Circuit breaker ativado. Convocando Assembleia Geral via Uniting for Peace.")

        # 3. Votação na AG: 2/3 dos 193 = 129 votos necessários
        votes_yes = 152  # simulação
        if votes_yes >= 129:
            self.lambda2_global = 0.72
            print(f"[SIM] Reforma aprovada pela AG. λ₂ sobe para {self.lambda2_global}")
        else:
            print("[SIM] Falha na AG. Sistema entra em modo de crise.")

        return self.lambda2_global

# Execução
if __name__ == "__main__":
    game = UNReformGame()
    nash = game.nash_equilibrium()
    print(f"Equilíbrio de Nash: {nash}")

    hist_RU, hist_G4 = game.replicator_dynamics()
    plt.plot(hist_RU, label='Rússia (Cooperar)')
    plt.plot(hist_G4, label='G4+UA (Cooperar)')
    plt.xlabel('Iterações')
    plt.ylabel('Probabilidade de cooperação')
    plt.title('Dinâmica de Replicador – Reforma do CSNU')
    plt.legend()
    plt.savefig('unsc_game_theory.png')

    final_lambda = game.simulate_blockage_scenario()
    print(f"λ₂ final após simulação: {final_lambda:.2f}")
