from river import optim
from river import utils
import numpy as np
import copy
import logging

LOG = logging.getLogger("root")


class SynchronousSGD(optim.Optimizer):
    def __init__(self, lr=0.01, model_name=None, neighbors=None):
        super().__init__(lr)
        if neighbors is None:
            neighbors = []
        self.model_name = model_name
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
            each_w = copy.deepcopy(neighbor.get_current_weights())
            each_g = copy.deepcopy(neighbor.get_current_gradients())
            if each_g is not None and each_w is not None:
                each_w = self._internal_step(each_w, each_g)
                if resultant_w is not None:
                    resultant_w += each_w
                else:
                    resultant_w = each_w

        self._internal_step(w, g)
        if resultant_w is not None:
            w += resultant_w

        self.current_gradients = copy.deepcopy(g)
        self.current_weights = copy.deepcopy(w)
        LOG.debug("[{}] g: {}, w: {}".format(self.model_name,
                                             self.current_gradients,
                                             self.current_weights))
        return w

