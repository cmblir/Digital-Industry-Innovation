from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:1234@211.245.172.58:5432/nia_test'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# import views after creating the app and db to avoid circular imports
import views

if __name__ == '__main__':
    app.run(debug=True, port=5015)
