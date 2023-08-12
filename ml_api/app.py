"""Flask App for ML API"""
import os
from flask import Flask, render_template, request, jsonify, send_from_directory
import json

# Define upload folder path:
UPLOAD_FOLDER = os.path.join("static",'uploads')
RESPONSE_FOLDER = os.path.join("response")
# Define allowed files:
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['RESPONSE_FOLDER'] = RESPONSE_FOLDER

@app.route("/")
def welcome():
    """Fashion MNIST API Landing Page."""
    return render_template("home.html")

@app.route("/upload", methods=["GET", "POST"])
def upload():
    """Upload image."""
    return render_template("upload.html")