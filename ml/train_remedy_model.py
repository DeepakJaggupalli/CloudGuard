from pathlib import Path
import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestClassifier

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data' / 'Data set' / 'vm_events_remedies_train_8000.csv'
MODEL = ROOT / 'ml' / 'models' / 'remedy_random_forest.joblib'

FEATURES = ['event_type', 'event_category', 'severity']
TARGET = 'target_label'


def main():
    df = pd.read_csv(DATA)
    X = df[FEATURES].fillna('unknown').astype(str)
    y = df[TARGET].fillna('unknown').astype(str)
    prep = ColumnTransformer([
        ('cat', OneHotEncoder(handle_unknown='ignore'), FEATURES),
    ])
    pipe = Pipeline([
        ('prep', prep),
        ('model', RandomForestClassifier(n_estimators=300, random_state=42, class_weight='balanced')),
    ])
    pipe.fit(X, y)
    MODEL.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, MODEL)
    print(f'Saved: {MODEL}')
    print(f'Rows: {len(df)} | Classes: {sorted(y.unique())}')


if __name__ == '__main__':
    main()
