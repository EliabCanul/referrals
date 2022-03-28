import pandas as pd
import numpy as np
import warnings

# Refer to read.me info
def mape(y_true, y_pred, remove_zero_targets = True):
    """_summary_

    :param y_true: _description_
    :type y_true: _type_
    :param y_pred: _description_
    :type y_pred: _type_
    :param remove_zero_targets: _description_, defaults to True
    :type remove_zero_targets: bool, optional
    :return: _description_
    :rtype: _type_
    """
    # remove_zeros
    if remove_zero_targets:
        if len(y_true) > 1:
            nonZeroIndices = np.array([ind for ind, val in enumerate(y_true == 0) if val == 0])
        else:
            return (np.abs(y_true - y_pred)/y_true)[0]
    else:
        for i in range(len(y_true)):
            if y_true[i] == 0:
                y_true[i] += 1
                y_pred[i] += 1

    return np.mean(np.abs((y_true[nonZeroIndices] - y_pred[nonZeroIndices]) / y_true[nonZeroIndices])) * 100


