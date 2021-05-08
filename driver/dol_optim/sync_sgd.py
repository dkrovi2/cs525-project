from river import optim
from river import utils
import numpy as np


class SynchronousSGD(optim.Optimizer):
    def __init__(self, lr=0.01, mylock=None):
        super().__init__(lr)
        self.mylock = mylock

    def _step(self, w, g):
        self.mylock.acquire()
        if isinstance(w, utils.VectorDict) and isinstance(g, utils.VectorDict):
            w -= self.learning_rate * g
        elif isinstance(w, np.ndarray) and isinstance(g, np.ndarray):
            w -= self.learning_rate * g
        else:
            for i, gi in g.items():
                w[i] -= self.learning_rate * gi
        self.mylock.release()
        return w

