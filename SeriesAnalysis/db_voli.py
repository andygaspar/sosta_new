import pandas as pd
import numpy as np


def make_db_voli(voli: pd.DataFrame):

    voli["-4"] = np.ones(voli.shape[0]).astype(int) * (-4)
    voli["-4p"] = np.zeros(voli.shape[0]).astype(int)

    voli["-3"] = np.ones(voli.shape[0]).astype(int) * (-3)
    voli["-3p"] = np.zeros(voli.shape[0]).astype(int)

    voli["-2"] = np.ones(voli.shape[0]).astype(int) * (-2)
    voli["-2p"] = np.zeros(voli.shape[0]).astype(int)

    voli["-1"] = np.ones(voli.shape[0]).astype(int) * (-1)
    voli["-1p"] = np.zeros(voli.shape[0]).astype(int)

    voli["0"] = np.zeros(voli.shape[0]).astype(int)
    voli["0p"] = np.ones(voli.shape[0]).astype(int)

    voli["1"] = np.ones(voli.shape[0]).astype(int) * 1
    voli["1p"] = np.zeros(voli.shape[0]).astype(int)

    voli["2"] = np.ones(voli.shape[0]).astype(int) * 2
    voli["2p"] = np.zeros(voli.shape[0]).astype(int)

    voli["3"] = np.ones(voli.shape[0]).astype(int) * 3
    voli["3p"] = np.zeros(voli.shape[0]).astype(int)

    voli["4"] = np.ones(voli.shape[0]).astype(int) * 4
    voli["4p"] = np.zeros(voli.shape[0]).astype(int)

    return voli
