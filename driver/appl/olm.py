from river import compose
from river import linear_model
from river import metrics
from river import preprocessing
from dol_optim.sync_sgd import SynchronousSGD

import json
import logging

LOG = logging.getLogger("root")


class OnlineModel:
    def __init__(self, step, name):
        self.name = name
        self.optimizer = SynchronousSGD(0.01, name, None)
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            linear_model.LogisticRegression(self.optimizer))
        self.metrics = [metrics.Accuracy(), metrics.MAE(), metrics.RMSE(), metrics.Precision(), metrics.Recall()]
        self.count = 0
        if step is None:
            self.step = 50
        else:
            self.step = int(step)

    def get_name(self):
        return self.name

    def set_neighbors(self, neighbors):
        self.optimizer.set_neighbors(neighbors)

    def get_current_gradients(self):
        # Assuming that optimizer is an instance of SynchronousSGD
        return self.optimizer.current_gradients

    def get_current_weights(self):
        # Assuming that optimizer is an instance of SynchronousSGD
        return self.optimizer.current_weights

    def get_current_metrics(self):
        return self.metrics

    def train(self, message, group):
        self.count = self.count + 1
        record = json.loads(message.strip())
        x, y = record
        y_pred = self.model.predict_one(x)
        for metric in self.metrics:
            metric.update(y, y_pred)
        self.model = self.model.learn_one(x, y)

