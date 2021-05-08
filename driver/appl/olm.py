from river import compose
from river import linear_model
from river import metrics
from river import preprocessing
from dol_optim.sync_sgd import SynchronousSGD

import json


class OnlineModel:
    def __init__(self):
        self.optimizer = SynchronousSGD(0.01, None)
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            linear_model.LogisticRegression(self.optimizer))
        self.metric_accuracy = metrics.Accuracy()
        self.metric_mae = metrics.MAE()
        self.metric_rmse = metrics.RMSE()
        self.count = 0

    def set_neighbors(self, neighbors):
        self.optimizer.set_neighbors(neighbors)

    def get_current_gradients(self):
        # Assuming that optimizer is an instance of SynchronousSGD
        return self.optimizer.current_gradients

    def train(self, message, group):
        self.count = self.count + 1
        record = json.loads(message.strip())
        x, y = record
        y_pred = self.model.predict_one(x)
        self.metric_accuracy = self.metric_accuracy.update(y, y_pred)
        self.metric_mae = self.metric_mae.update(y, y_pred)
        self.metric_rmse = self.metric_rmse.update(y, y_pred)
        self.model = self.model.learn_one(x, y)
        if self.count % 10 ==0:
            print("[{0}-{1}] {2} {3} {4}".format(group,
                                                 self.count,
                                                 self.metric_accuracy,
                                                 self.metric_mae,
                                                 self.metric_rmse))

