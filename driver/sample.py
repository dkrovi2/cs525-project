from river import compose
from river import preprocessing
from river import linear_model
from river import metrics
from river import datasets
from river import optim

optimizer = optim.SGD(0.1)
model = compose.Pipeline(
    preprocessing.StandardScaler(),
    linear_model.LogisticRegression(optimizer)
)

metric = metrics.ROCAUC()
precision = metrics.Precision()


for x, y in datasets.Phishing():
    y_pred = model.predict_proba_one(x)
    model.learn_one(x, y)
    metric.update(y, y_pred)
    precision.update(y, y_pred)

print(metric)
print(precision)
