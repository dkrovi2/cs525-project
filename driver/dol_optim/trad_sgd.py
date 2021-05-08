from river import optim
from river import utils
import numpy as np


class TraditionalSGD(optim.Optimizer):
    def __init__(self, lr=0.01, neighbors=None):
        super().__init__(lr)
        if neighbors is None:
            neighbors = []
        self.current_gradients = None
        # Each neighbor is an instance of OnlineModel
        self.neighbors = neighbors

    def set_neighbors(self, neighbors):
        self.neighbors = neighbors

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
        all_g = []
        resultant_g = g
        # If there are neighbors, get the gradients from the neighbors and
        # append them to the list.
        if self.neighbors:
            # Get the weights from each neighbor and perform the "consolidation"
            for neighbor in self.neighbors:
                each_g = neighbor.get_current_gradients()
                if each_g:
                    all_g.append(each_g)
                    for key in each_g.keys():
                        if key in resultant_g:
                            resultant_g[key] += each_g[key]
                        else:
                            resultant_g[key] = each_g[key]

            for key in resultant_g.keys():
                resultant_g[key] /= len(self.neighbors)

        if isinstance(w, utils.VectorDict) and isinstance(resultant_g, utils.VectorDict):
            w -= self.learning_rate * resultant_g
        elif isinstance(w, np.ndarray) and isinstance(resultant_g, np.ndarray):
            w -= self.learning_rate * resultant_g
        else:
            for i, gi in resultant_g.items():
                w[i] -= self.learning_rate * gi

        self.current_gradients = resultant_g
        return self._internal_step(w, resultant_g)
