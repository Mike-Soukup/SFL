import tensorflow as tf 
from tensorflow import keras
import numpy as np
from PIL import Image

def load_model(model_path: str):
    """Driver function to load in our model"""
    model = keras.models.load_model(model_path)
    return model

def make_prediction(img_path):
    '''Driver funtion to make prediction on input Fashion MNIST image'''
    ### Model Labels:
    class_names = ['T-shirt/top', 'Trouser', 'Pullover', 'Dress', 'Coat',
            'Sandal', 'Shirt', 'Sneaker', 'Bag', 'Ankle Boot']
    ### Load Fashion MNIST Model
    model_path = r'./model/fashion_mnist.h5'
    model = load_model(model_path)
    ### Load in the image:
    with Image.open(img_path) as img:
        img.load()
    ### Convert to array:
    im_arr = np.asarray(img)
    im_arr = im_arr/255.0
    im_arr = np.reshape(im_arr, (1,28,28))
    pred = np.argmax(model.predict(im_arr), axis=-1)
    pred = np.array(class_names)[pred][0]
    return pred
