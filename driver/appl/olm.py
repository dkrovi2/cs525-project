from river import compose
from river import linear_model
from river import metrics
from river import preprocessing
# from dol_optim.trad_sgd import TraditionalSGD
from dol_optim.sync_sgd import SynchronousSGD
from river.optim import SGD

import json
import logging

LOG = logging.getLogger("root")


class OnlineModel:
    def __init__(self, step):
        self.optimizer = SynchronousSGD(0.01, None)
        # self.optimizer = SGD(0.01)
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            linear_model.LogisticRegression(self.optimizer))
        self.metrics = [metrics.Accuracy(), metrics.MAE(), metrics.RMSE(), metrics.Precision(), metrics.Recall()]
        self.count = 0
        if step is None:
            self.step = 50
        else:
            self.step = int(step)

    def set_neighbors(self, neighbors):
        self.optimizer.set_neighbors(neighbors)

    def get_current_gradients(self):
        # Assuming that optimizer is an instance of SynchronousSGD
        return self.optimizer.current_gradients

    def get_current_weights(self):
        # Assuming that optimizer is an instance of SynchronousSGD
        return self.optimizer.current_weights

    def train(self, message, group):
        self.count = self.count + 1
        record = json.loads(message.strip())
        x, y = record
        y_pred = self.model.predict_one(x)
        for metric in self.metrics:
            metric.update(y, y_pred)
        self.model = self.model.learn_one(x, y)
        if self.count % self.step == 0:
            metric_log = ""
            for metric in self.metrics:
                metric_log = metric_log + str(metric) + " | "
            LOG.info("[{0}-{1}] {2}".format(group, self.count, metric_log.strip()))

