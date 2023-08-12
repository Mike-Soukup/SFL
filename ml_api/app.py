"""Flask App for ML API"""
import os
from flask import Flask, render_template, request, jsonify, send_from_directory
import json
from werkzeug.utils import secure_filename
from fileinput import filename
from img_etl_pred import load_model, make_prediction

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

@app.route("/output", methods=["POST"])
def output():
    """Return output of image submission."""
    if request.method == "POST":
        # Get uploaded files
        f1 = request.files["img1"]

        # Extract uploaded data files
        img1_filename = secure_filename(f1.filename)

        # Upload file:
        f1.save(os.path.join(app.config['UPLOAD_FOLDER'], img1_filename))

        # Create file path:
        img1_path = os.path.join(app.config['UPLOAD_FOLDER'], img1_filename)

        #Make Prediction
        prediction = make_prediction(img1_path)

        # Create JSON-like output to simulate REST API:
        rest = {'predicted_class':prediction}

        # # File Cleanup:
        # os.remove(os.path.join(app.config['UPLOAD_FOLDER'], img1_path))

        return render_template(
            "output.html",
            image_1=img1_path,
            prediction = prediction,
            rest = rest,
        )
if __name__ == "__main__":
    app.run(host="0.0.0.0", debug = True)