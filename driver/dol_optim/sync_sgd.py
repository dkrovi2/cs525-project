from river import optim
from river import utils
import numpy as np


class SynchronousSGD(optim.Optimizer):
    def __init__(self, lr=0.01, neighbors=None):
        super().__init__(lr)
        if neighbors is None:
            neighbors = []
        self.current_weights = None
        self.current_gradients = None
        # Each neighbor is an instance of OnlineModel
        self.neighbors = neighbors

    def set_neighbors(self, neighbors):
        self.neighbors = neighbors

    def get_current_weights(self):
        return self.current_weights

    def get_current_gradients(self):
        return self.current_gradients

    def _internal_step(self, w, g):
        if isinstance(w, utils.VectorDict) and isinstance(g, utils.VectorDict):
            w -= self.learning_rate * g
        elif isinstance(w, np.ndarray) and isinstance(g, np.ndarray):
            w -= self.learning_rate * g
        else:
            for i, gi in g.items():
                w[i] -= self.learning_rate * gi

        return w

    def _step(self, w, g):
        if not self.neighbors:
            w = self._internal_step(w, g)
            self.current_weights = w
            self.current_gradients = g
            return w

        # w(t+1) = sum(w(t) - lr*g(t))
        # Get the weights from each neighbor and perform the "consolidation"
        resultant_w = None
        for neighbor in self.neighbors:
            each_w = neighbor.get_current_weights()
            each_g = neighbor.get_current_gradients()
            if each_g and each_w:
                each_w_t1 = self._internal_step(each_w, each_g)
                if resultant_w:
                    resultant_w += each_w_t1
                else:
                    resultant_w = each_w_t1

        self._internal_step(w, g)
        if resultant_w:
            w += resultant_w

        self.current_gradients = g
        self.current_weights = w
        return w

