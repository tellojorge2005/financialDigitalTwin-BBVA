import numpy as np
import pandas as pd
import pytest

from etl.src.monte_carlo import simular_historial


@pytest.fixture
def historial():
    return pd.DataFrame(
        {
            "client_id": [1] * 6,
            "currency": ["MXN"] * 6,
            "month": pd.date_range("2026-01-01", periods=6, freq="MS"),
            "monthly_income": [1000.0] * 6,
            "monthly_expenses": [600.0] * 6,
            "monthly_debt_payments": [100.0] * 6,
            "closing_balance": [200.0] * 6,
        }
    )


# Sin variación, los resultados deben coincidir con el cálculo directo
def test_saldos_dos_y_tres_meses(historial):
    resultado = simular_historial(historial, escenarios=100)
    assert resultado["horizon_months"].tolist() == [1, 2, 3]
    np.testing.assert_allclose(resultado["balance_p50"], [500, 800, 1100])
    np.testing.assert_allclose(resultado["income_p50"], [1000] * 3)
    np.testing.assert_allclose(resultado["expenses_p50"], [600] * 3)
    assert resultado["prediction_month"].astype(str).tolist() == [
        "2026-07-01",
        "2026-08-01",
        "2026-09-01",
    ]
    assert resultado["prob_negative_balance"].eq(0).all()


def test_historial_insuficiente_no_inventa_resultados(historial):
    resultado = simular_historial(historial.tail(3), escenarios=100)
    assert resultado["status"].eq("HISTORIAL_INSUFICIENTE").all()
    assert resultado["balance_p50"].isna().all()
    assert resultado["scenarios"].eq(0).all()


@pytest.mark.parametrize("caso", ["moneda", "duplicado", "discontinuo", "importe"])
def test_rechaza_historial_invalido(historial, caso):
    if caso == "moneda":
        historial["currency"] = "USD"
    elif caso == "duplicado":
        historial = pd.concat([historial, historial.iloc[[0]]])
    elif caso == "discontinuo":
        historial = historial.drop(index=2)
    else:
        historial.loc[0, "monthly_expenses"] = -1
    with pytest.raises(ValueError):
        simular_historial(historial, escenarios=100)
