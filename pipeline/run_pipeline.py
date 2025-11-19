
import pandas as pd
from data_preprocessing import clean_data, transform_data
from feature_engineering import create_features, select_features
from model_training import train_model
from model_evaluation import evaluate_model

def run_pipeline(data, target):
    # Paso 1: Limpieza de los datos
    data_cleaned = clean_data(data)
    
    # Paso 2: Transformación de los datos
    data_transformed = transform_data(data_cleaned)
    
    # Paso 3: Ingeniería de características
    data_with_features = create_features(data_transformed)
    data_selected = select_features(data_with_features, target)
    
    # Paso 4: Entrenamiento del modelo
    model = train_model(data_selected, target)
    
    # Paso 5: Evaluación del modelo
    y_true = data_selected[target]
    y_pred = model.predict(data_selected.drop(target, axis=1))
    evaluate_model(y_true, y_pred)
