from river import compose
from river import linear_model
from river import metrics
from river import preprocessing

import json


class OnlineModel:
    def __init__(self, optimizer):
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            linear_model.LogisticRegression(optimizer))
        self.metric = metrics.Accuracy()
        self.count = 0

    def train(self, message, group):
        self.count = self.count + 1
        record = json.loads(message.strip())
        x, y = record
        y_pred = self.model.predict_one(x)
        self.metric = self.metric.update(y, y_pred)
        self.model = self.model.learn_one(x, y)
        print("[{0}-{1}] {2}".format(group, self.count, self.metric))

