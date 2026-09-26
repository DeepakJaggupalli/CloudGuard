from pathlib import Path
import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from features import FEATURES

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data' / 'Data set' / 'vm_monitoring_train_8000.csv'
MODEL = ROOT / 'ml' / 'models' / 'isolation_forest.joblib'


def main():
    df = pd.read_csv(DATA)
    X = df[FEATURES].apply(pd.to_numeric, errors='coerce').fillna(0)
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('model', IsolationForest(n_estimators=250, contamination=0.10, random_state=42)),
    ])
    pipe.fit(X)
    MODEL.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, MODEL)
    print(f'Saved: {MODEL}')
    print(f'Rows: {len(df)} | Features: {FEATURES}')


if __name__ == '__main__':
    main()
