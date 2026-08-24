from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter


def _format_decimal_pt(value, decimals=1):
    """Formata número usando vírgula como separador decimal."""
    return f"{value:.{decimals}f}".replace(".", ",")


def _format_brl_thousands(value, _position=None):
    """Formata valores monetários do eixo em milhares de reais."""
    return f"R$ {value / 1000:.0f} mil"


def _save_figure(fig, output_dir, filename, dpi=300):
    """
    Salva a figura em PDF vetorial e PNG.

    O PDF é recomendado para inclusão no LaTeX.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = output_dir / f"{filename}.pdf"
    png_path = output_dir / f"{filename}.png"

    fig.savefig(
        pdf_path,
        bbox_inches="tight",
    )

    fig.savefig(
        png_path,
        dpi=dpi,
        bbox_inches="tight",
    )

    plt.close(fig)

    return pdf_path, png_path


def plot_cap4_ablacao_historico(output_dir):
    """
    Figura:
    ganho no MAE obtido pelas diferentes famílias de variáveis
    em relação à configuração inicial do HGB.

    Referência:
    HGB com conjunto inicial de variáveis
    MAE = 47.303739 h.
    """

    mae_original = 47.303739

    labels = [
        "Estrutura",
        "Estrutura + tempo",
        "Estrutura + clima",
        "Estrutura + tempo + clima",
        "Histórico do porto",
        "Estado operacional D-1",
        "Histórico de rota e embarcação",
        "Modelo completo",
    ]

    mae = np.array([
        47.570,
        47.300,
        47.495,
        47.344,
        47.413,
        47.257,
        44.535027,
        44.535027,
    ])

    ganho_pct = (
        (mae_original - mae)
        / mae_original
        * 100
    )

    fig, ax = plt.subplots(
        figsize=(8.4, 4.8)
    )

    bars = ax.barh(
        labels,
        ganho_pct,
    )

    # Linha de referência em zero.
    ax.axvline(
        0,
        linewidth=0.8,
    )

    ax.set_xlabel(
        "Ganho no MAE em relação à configuração inicial (%)"
    )

    ax.set_ylabel("")

    # Mantém a primeira categoria no topo.
    ax.invert_yaxis()

    # Espaço suficiente para os rótulos da direita.
    ax.set_xlim(
        -1.0,
        6.8,
    )

    ax.grid(
        axis="x",
        alpha=0.25,
    )

    # ---------------------------------------------------------
    # RÓTULOS
    #
    # Todos os valores ficam do lado direito das barras.
    #
    # Para barras positivas:
    # o texto fica depois do final da barra.
    #
    # Para barras negativas:
    # o texto fica à direita da linha zero.
    # ---------------------------------------------------------

    for bar, value in zip(
        bars,
        ganho_pct,
    ):
        y = (
            bar.get_y()
            + bar.get_height() / 2
        )

        if value > 0:
            x_text = value + 0.10
        else:
            x_text = 0.10

        ax.text(
            x_text,
            y,
            f"{_format_decimal_pt(value, 2)}%",
            va="center",
            ha="left",
            fontsize=9,
        )

    fig.tight_layout()

    return _save_figure(
        fig,
        output_dir,
        "cap4_ablacao_familias_historicas",
    )


def plot_cap4_quintis_incerteza(output_dir):
    """
    Figura:
    relação entre amplitude prevista P95-P50,
    erro absoluto do P50 e permanência observada.
    """

    quintis = [
        "Q1",
        "Q2",
        "Q3",
        "Q4",
        "Q5",
    ]

    erro_p50 = np.array([
        8.744,
        20.693,
        33.108,
        48.497,
        82.407,
    ])

    permanencia = np.array([
        25.431,
        46.091,
        62.432,
        83.222,
        134.816,
    ])

    fig, ax = plt.subplots(
        figsize=(8.0, 4.6)
    )

    ax.plot(
        quintis,
        erro_p50,
        marker="o",
        linewidth=2,
        label="Erro absoluto médio do P50",
    )

    ax.plot(
        quintis,
        permanencia,
        marker="o",
        linewidth=2,
        label="Permanência média observada",
    )

    ax.set_xlabel(
        "Quintil da amplitude prevista P95-P50\n"
        "(Q1 = menor amplitude; Q5 = maior amplitude)"
    )

    ax.set_ylabel(
        "Horas"
    )

    # Espaço adicional no topo para não encostar o 134,8 na borda.
    ax.set_ylim(
        0,
        148,
    )

    ax.grid(
        axis="y",
        alpha=0.25,
    )

    ax.legend(
        frameon=False
    )

    # ---------------------------------------------------------
    # Rótulos da linha azul
    # ---------------------------------------------------------

    for x, y in zip(
        quintis,
        erro_p50,
    ):
        ax.annotate(
            _format_decimal_pt(y, 1),
            (x, y),
            xytext=(0, 7),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
        )

    # ---------------------------------------------------------
    # Rótulos da linha laranja
    # ---------------------------------------------------------

    for i, (x, y) in enumerate(
        zip(
            quintis,
            permanencia,
        )
    ):

        if i == len(permanencia) - 1:
            # Último ponto (Q5 / 134,8):
            # desloca para esquerda e para baixo.
            xytext = (-18, -10)
            ha = "right"
            va = "top"

        else:
            xytext = (0, 7)
            ha = "center"
            va = "bottom"

        ax.annotate(
            _format_decimal_pt(y, 1),
            (x, y),
            xytext=xytext,
            textcoords="offset points",
            ha=ha,
            va=va,
            fontsize=8,
        )

    fig.tight_layout()

    return _save_figure(
        fig,
        output_dir,
        "cap4_quintis_incerteza",
    )


def plot_cap4_tradeoff_capital_cobertura(output_dir):
    """
    Figura:
    trade-off entre capital associado ao estoque de segurança
    e taxa de cobertura do lead time.
    """

    policies = [
        "P90 histórico",
        "Pontual inicial",
        "Pontual enriquecido",
        "P90 dinâmico",
        "P95 dinâmico",
    ]

    capital = np.array([
        37988.6648,
        11951.0732,
        13116.2450,
        36364.5929,
        44352.8260,
    ])

    cobertura = np.array([
        89.5534,
        52.8712,
        52.2665,
        89.4772,
        94.4053,
    ])

    fig, ax = plt.subplots(
        figsize=(8.2, 5.0)
    )

    # Cada política é desenhada separadamente,
    # permitindo diferenciação visual automática.
    for policy, x, y in zip(
        policies,
        capital,
        cobertura,
    ):
        ax.scatter(
            x,
            y,
            s=70,
            label=policy,
        )

    # Ajustes manuais dos rótulos.
    offsets = {
        "P90 histórico": (8, 8),
        "Pontual inicial": (-5, 10),
        "Pontual enriquecido": (8, -15),
        "P90 dinâmico": (-70, -16),
        "P95 dinâmico": (-70, 9),
    }

    for policy, x, y in zip(
        policies,
        capital,
        cobertura,
    ):
        dx, dy = offsets[policy]

        ax.annotate(
            policy,
            (x, y),
            xytext=(dx, dy),
            textcoords="offset points",
            fontsize=9,
        )

    ax.set_xlabel(
        "Capital associado ao estoque de segurança"
    )

    ax.set_ylabel(
        "Taxa de cobertura do lead time (%)"
    )

    ax.xaxis.set_major_formatter(
        FuncFormatter(
            _format_brl_thousands
        )
    )

    ax.set_ylim(
        45,
        100,
    )

    ax.grid(
        alpha=0.25
    )

    # Os nomes já estão diretamente nos pontos.
    # A legenda seria redundante.
    legend = ax.get_legend()

    if legend is not None:
        legend.remove()

    fig.tight_layout()

    return _save_figure(
        fig,
        output_dir,
        "cap4_tradeoff_capital_cobertura",
    )


def gerar_figuras_cap4(output_dir):
    """
    Gera todas as figuras finais do Capítulo 4.

    São salvos arquivos PDF e PNG.
    """

    arquivos = {}

    arquivos["ablacao_historico"] = (
        plot_cap4_ablacao_historico(
            output_dir
        )
    )

    arquivos["quintis_incerteza"] = (
        plot_cap4_quintis_incerteza(
            output_dir
        )
    )

    arquivos["tradeoff_capital_cobertura"] = (
        plot_cap4_tradeoff_capital_cobertura(
            output_dir
        )
    )

    return arquivos