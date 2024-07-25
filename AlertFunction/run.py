import azure.functions as func
import logging
from AlertFunction.params import prod_params,local_params
from AlertFunction.constant import restaurants
import pandas as pd
from AlertFunction.Functions.prediction_deviation import deviation_in_prediction
from AlertFunction.Functions.opening_hours import opening_hours_diff,prediction_difference,prediction_restaurant_count
from AlertFunction.Functions.send_email import send_email
from dotenv import load_dotenv
import os

RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL')

def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")
    pred_rest_count = prediction_restaurant_count()
    differences_opening_hours = opening_hours_diff()
    difference_predictions = prediction_difference()
    deviation = deviation_in_prediction()

    # deviation.to_csv('deviation.csv')
    # if len(differences_opening_hours) :
    #     differences_opening_hours.to_csv("new_opening_hours.csv")
    # if len(difference_predictions) > 0:
    #     difference_predictions.to_csv("unusual_pred.csv")

    # Convert DataFrames to HTML
    prediction_restaurant_count_html = pred_rest_count.to_html()
    deviation_html = deviation.to_html()
    differences_opening_hours_html = differences_opening_hours.to_html() if len(differences_opening_hours) >0 else "There are currently no changes in opening hours"
    difference_predictions_html = difference_predictions.to_html() if len(difference_predictions) > 0 else ""


    # Create email content
    email_subject = "Restaurant Data Updates"
    email_body = f"""
    <h3>Today, predictions were successfull on following restaurants</h3>
    {prediction_restaurant_count_html}
    <br><br>
    <h3>Unusual Predictions</h3>
    {difference_predictions_html}
    <br><br>
    <h3>Deviation in Prediction</h3>
    {deviation_html}
    <br><br>
    <h3>New Opening Hours</h3>
    {differences_opening_hours_html}
    """

    # Send the email
    send_email(email_subject, email_body, RECIPIENT_EMAIL)



                

            