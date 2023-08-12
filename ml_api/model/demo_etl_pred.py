import tensorflow as tf 
from tensorflow import keras
import numpy as np

def load_model(model_path: str):
    """Driver function to load in our model"""
    model = keras.models.load_model(model_path)
    return model


if __name__ == '__main__':
    path = "fashion_mnist.h5"
    class_names = ['T-shirt/top', 'Trouser', 'Pullover', 'Dress', 'Coat',
               'Sandal', 'Shirt', 'Sneaker', 'Bag', 'Ankle Boot']
    model = load_model(path)
    ### Load Fashion MNIST Data:
    fashion_mnist = keras.datasets.fashion_mnist
    (X,y), _ = fashion_mnist.load_data()
    ### Scale Image Data:
    X_scaled = X / 255.0
    ### Take subset for preditions:
    X_pred = X_scaled[:10]
    y_pred = np.argmax(model.predict(X_pred), axis=-1)
    print(np.array(class_names)[y_pred])



