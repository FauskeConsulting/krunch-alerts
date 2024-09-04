import azure.functions as func
import logging
import pandas as pd
from AlertFunction.Functions.prediction_deviation import deviation_in_prediction
from AlertFunction.Functions.salesvpred import sales_vs_pred
from AlertFunction.Functions.opening_hours import opening_hours_diff,prediction_difference,prediction_restaurant_count
from AlertFunction.Functions.send_email import send_email
from dotenv import load_dotenv
import os

RECIPIENT_EMAIL = os.environ.get('RECIPIENT_EMAILS').split(',')

def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")
    pred_rest_count = prediction_restaurant_count()
    # differences_opening_hours = opening_hours_diff()
    difference_predictions = prediction_difference()
    deviation = deviation_in_prediction()
    sales_pred = sales_vs_pred()
    # pred_rest_count.to_csv('preddate.csv')
    # differences_opening_hours.to_csv('predopeninghrs.csv')
    # difference_predictions.to_csv('preddiff.csv')
    # deviation.to_csv('pred_deviation.csv')
    # sales_pred.to_csv('predvSales.csv')


    # Convert DataFrames to HTML
    prediction_restaurant_count_html = pred_rest_count.to_html(index=False)
    deviation_html = deviation.to_html(index=False)
    # differences_opening_hours_html = differences_opening_hours.to_html(index=False) if len(differences_opening_hours) >0 else "There are currently no changes in opening hours"
    difference_predictions_html = difference_predictions.to_html(index=False) if len(difference_predictions) > 0 else "There are currently no unusual predictions that deviate more than 50% since the last prediction"
    sales_pred_html = sales_pred.to_html(index=False)

    # Create email content
    email_subject = "Restaurant Data Updates"
    email_body = f"""
    <h3>Prediction Status</h3>
    <p>The table shows when the prediction ran for which restaurant</p>
    {prediction_restaurant_count_html}
    <br><br>
    <h3>Unusual Predictions</h3>
    {difference_predictions_html}
    <br><br>
    <h3>Actual Sales vs Prediction</h3>
    {sales_pred_html}
    <br><br>
    <h3>trend in Prediction for the next two weeks</h3>
    <p>This table shows the next two weeks of predictions where <strong>first_total_gross </strong> is the prediction amount of the first prediction run </p>
    <p> whereas <strong>last_total_gross </strong> is the prediction amount of the latest prediction run which might have changed because of updated sales and weather data </p>
    {deviation_html}
    <br><br>
    """

    # Send the email
    send_email(email_subject, email_body, RECIPIENT_EMAIL)



                

            