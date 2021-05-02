from river import linear_model
from river import optim
from river import preprocessing
from sklearn import datasets
from sklearn import metrics
from river import stream

scaler = preprocessing.StandardScaler()
optimizer = optim.SGD(lr=0.01)
log_reg = linear_model.LogisticRegression(optimizer)

y_true = []
y_pred = []

for xi, yi in stream.iter_sklearn_dataset(datasets.load_breast_cancer(), shuffle=True, seed=42):

    # Scale the features
    xi_scaled = scaler.learn_one(xi).transform_one(xi)

    # Test the current model on the new "unobserved" sample
    yi_pred = log_reg.predict_proba_one(xi_scaled)
    # Train the model with the new sample
    log_reg.learn_one(xi_scaled, yi)

    # Store the truth and the prediction
    y_true.append(yi)
    y_pred.append(yi_pred[True])

print(f'ROC AUC: {metrics.roc_auc_score(y_true, y_pred):.3f}')