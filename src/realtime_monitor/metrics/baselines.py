from __future__ import annotations

import math


def mean_std(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return mean, math.sqrt(variance)


def z_score(value: float, values: list[float]) -> float:
    mean, std = mean_std(values)
    if std == 0:
        return 0.0
    return (value - mean) / std
