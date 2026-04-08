# game_theory_unsc_corrected.py
import numpy as np
import matplotlib.pyplot as plt

class UNReformGameCorrected:
    def __init__(self):
        # Estratégias: 0 = Cooperar, 1 = Bloquear, 2 = Criar bloco alternativo
        # Payoffs para Rússia (P5_RU) e para o bloco G4+UA
        self.payoffs = {
            ('P5_RU', 'Cooperar'): {'self': 30, 'other': 80, 'global': 60},
            ('P5_RU', 'Bloquear'): {'self': 50, 'other': 10, 'global': 30},
            ('P5_RU', 'Alternativo'): {'self': 70, 'other': 20, 'global': 10},
            ('G4_UA', 'Cooperar'): {'self': 80, 'other': 30, 'global': 60},
            ('G4_UA', 'Bloquear'): {'self': 10, 'other': 50, 'global': 30},
            ('G4_UA', 'Alternativo'): {'self': 20, 'other': 70, 'global': 10}
        }
        self.lambda2_global = 0.59
        self.history = []

    def nash_equilibrium_3x3(self):
        """Encontra equilíbrios de Nash para jogos 3x3 (simplificado)"""
        # Matriz de payoff para Rússia (linhas) e G4 (colunas)
        # Linhas: 0=Cooperar, 1=Bloquear, 2=Alternativo
        # Colunas: 0=Cooperar, 1=Bloquear, 2=Alternativo
        payoff_RU = np.array([
            [30, 50, 70],   # RU coopera vs G4 coopera/bloqueia/alternativo
            [50, 50, 70],   # RU bloqueia vs ...
            [70, 70, 70]    # RU alternativo vs ...
        ])
        payoff_G4 = np.array([
            [80, 10, 20],   # G4 coopera vs RU coopera/bloqueia/alternativo
            [30, 10, 20],   # G4 bloqueia vs ...
            [30, 10, 20]    # G4 alternativo vs ...
        ])

        nash = []
        for i in range(3):
            for j in range(3):
                # Verifica se i é melhor resposta para RU dado j
                best_ru = np.max(payoff_RU[:, j])
                is_br_ru = (payoff_RU[i, j] == best_ru)
                # Verifica se j é melhor resposta para G4 dado i
                best_g4 = np.max(payoff_G4[i, :])
                is_br_g4 = (payoff_G4[i, j] == best_g4)
                if is_br_ru and is_br_g4:
                    nash.append((i, j))
        return nash

    def replicator_dynamics_3x3(self, steps=200):
        """Dinâmica de replicador para 3 estratégias (Rússia) e 3 (G4)"""
        # Probabilidades iniciais (Rússia)
        x_RU = np.array([0.6, 0.3, 0.1])  # [cooperar, bloquear, alternativo]
        # Probabilidades iniciais (G4)
        x_G4 = np.array([0.5, 0.2, 0.3])

        history_RU = [x_RU.copy()]
        history_G4 = [x_G4.copy()]

        # Matriz de payoff
        payoff_RU = np.array([
            [30, 50, 70],
            [50, 50, 70],
            [70, 70, 70]
        ])
        payoff_G4 = np.array([
            [80, 10, 20],
            [30, 10, 20],
            [30, 10, 20]
        ])

        for _ in range(steps):
            # Cálculo do payoff esperado para cada estratégia da Rússia
            exp_RU = payoff_RU @ x_G4
            avg_RU = np.sum(x_RU * exp_RU)
            dx_RU = x_RU * (exp_RU - avg_RU)
            x_RU = x_RU + 0.05 * dx_RU  # taxa de aprendizado
            x_RU = np.clip(x_RU, 0, 1)
            x_RU = x_RU / np.sum(x_RU)  # normalização

            # Cálculo do payoff esperado para cada estratégia do G4
            exp_G4 = payoff_G4.T @ x_RU
            avg_G4 = np.sum(x_G4 * exp_G4)
            dx_G4 = x_G4 * (exp_G4 - avg_G4)
            x_G4 = x_G4 + 0.05 * dx_G4
            x_G4 = np.clip(x_G4, 0, 1)
            x_G4 = x_G4 / np.sum(x_G4)

            history_RU.append(x_RU.copy())
            history_G4.append(x_G4.copy())

        return np.array(history_RU), np.array(history_G4)

# Execução
if __name__ == "__main__":
    game = UNReformGameCorrected()
    nash = game.nash_equilibrium_3x3()
    print(f"Equilíbrios de Nash (índices: 0=Cooperar, 1=Bloquear, 2=Alternativo): {nash}")
    # Esperado: (2,0) e (2,1) e (2,2) — Rússia sempre alternativo

    hist_RU, hist_G4 = game.replicator_dynamics_3x3(steps=200)

    plt.figure(figsize=(12, 5))
    plt.subplot(1, 2, 1)
    plt.plot(hist_RU[:,0], label='Cooperar')
    plt.plot(hist_RU[:,1], label='Bloquear')
    plt.plot(hist_RU[:,2], label='Alternativo')
    plt.title('Dinâmica de Estratégias – Rússia')
    plt.xlabel('Iterações')
    plt.ylabel('Probabilidade')
    plt.legend()

    plt.subplot(1, 2, 2)
    plt.plot(hist_G4[:,0], label='Cooperar')
    plt.plot(hist_G4[:,1], label='Bloquear')
    plt.plot(hist_G4[:,2], label='Alternativo')
    plt.title('Dinâmica de Estratégias – G4+UA')
    plt.xlabel('Iterações')
    plt.ylabel('Probabilidade')
    plt.legend()

    plt.tight_layout()
    plt.savefig('unsc_game_theory_corrected.png', dpi=150)
    plt.show()

    print("Gráfico gerado: unsc_game_theory_corrected.png")
