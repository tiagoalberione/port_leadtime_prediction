"""Modelos mínimos usados no experimento do Capítulo 4.

O objetivo deste módulo é evitar um catálogo de algoritmos. Mantemos apenas três
famílias com papéis fáceis de explicar academicamente:

- Ridge: referência linear regularizada;
- Random Forest: modelo baseado em várias árvores independentes;
- Gradient Boosting: árvores sequenciais que corrigem erros anteriores.

Os modelos quantílicos usam Gradient Boosting com função de perda quantílica.
"""

from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge


def build_ridge_model(alpha: float = 1.0) -> Ridge:
    """Cria o modelo Ridge usado como referência linear.

    No Capítulo 4, o Ridge deve ser treinado sobre `log1p(target)` e receber
    variáveis numéricas padronizadas no pipeline de preprocessamento. A
    regularização L2 reduz coeficientes excessivos e ajuda quando há muitas
    colunas após o one-hot encoding.

    Parameters
    ----------
    alpha:
        Intensidade da regularização L2. O valor 1.0 é o ponto de partida do
        experimento e pode ser ajustado apenas com o conjunto de validação.

    Returns
    -------
    sklearn.linear_model.Ridge
        Estimador ainda não treinado.
    """
    return Ridge(alpha=alpha)


def build_random_forest_model() -> RandomForestRegressor:
    """Cria o Random Forest usado na comparação de modelos pontuais.

    O Random Forest representa uma alternativa não linear capaz de capturar
    interações entre porto, calendário, operação e variáveis históricas. O
    `min_samples_leaf=5` reduz a tendência de criar folhas extremamente
    específicas para poucos registros.

    Returns
    -------
    sklearn.ensemble.RandomForestRegressor
        Estimador ainda não treinado.
    """
    return RandomForestRegressor(
        n_estimators=300,
        max_depth=None,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )


def build_gradient_boosting_model() -> GradientBoostingRegressor:
    """Cria o Gradient Boosting usado na comparação de modelos pontuais.

    O modelo combina árvores rasas sequencialmente. Cada nova árvore tenta
    corrigir parte do erro deixado pelas anteriores, oferecendo uma alternativa
    não linear ao Random Forest com comportamento diferente de aprendizado.

    Returns
    -------
    sklearn.ensemble.GradientBoostingRegressor
        Estimador ainda não treinado.
    """
    return GradientBoostingRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=3,
        random_state=42,
    )


def build_quantile_model(quantile: float) -> GradientBoostingRegressor:
    """Cria um Gradient Boosting para estimar um quantil do lead time.

    Diferentemente da regressão pontual, o objetivo aqui é estimar um percentil
    condicional, como P50, P90 ou P95. Esses valores serão usados para discutir
    risco de cauda e a simulação de estoque de segurança.

    Parameters
    ----------
    quantile:
        Percentil desejado em escala de 0 a 1. Para este TCC, os valores
        esperados são 0.50, 0.90 e 0.95.

    Returns
    -------
    sklearn.ensemble.GradientBoostingRegressor
        Estimador quantílico ainda não treinado.
    """
    if not 0 < quantile < 1:
        raise ValueError("quantile deve estar entre 0 e 1.")

    return GradientBoostingRegressor(
        loss="quantile",
        alpha=quantile,
        n_estimators=300,
        learning_rate=0.05,
        max_depth=3,
        random_state=42,
    )
