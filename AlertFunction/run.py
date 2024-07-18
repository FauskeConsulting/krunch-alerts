import azure.functions as func
import logging
import psycopg2
from AlertFunction.params import prod_params,local_params
from AlertFunction.constant import restaurants
import pandas as pd



def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer trigger function executed.")

    opening_hours_local = pd.DataFrame(columns=['start_hour', 'end_hour', 'start_date', 'end_date', 'restaurant', 'day_of_week'])
    opening_hours_prod = pd.DataFrame(columns=['start_hour', 'end_hour', 'start_date', 'end_date', 'restaurant', 'day_of_week'])

    with psycopg2.connect(**local_params) as conn:
        with conn.cursor() as cursor:
            for restaurant in restaurants:
                local_query_opening_hours = '''
                SELECT oh.start_hour, oh.end_hour, oh.start_date, oh.end_date, ar.name, oh.day_of_week
                FROM public.accounts_openinghours oh
                JOIN public.accounts_restaurant ar 
                ON ar.id = oh.restaurant_id
                WHERE ar.name = %s
                '''
                cursor.execute(local_query_opening_hours, (restaurant,))
                rows = cursor.fetchall()
                # Create a temporary dataframe for each restaurant's data
                temp_df = pd.DataFrame(rows, columns=['start_hour', 'end_hour', 'start_date', 'end_date', 'restaurant', 'day_of_week'])
                
                # Concatenate to the main dataframe
                opening_hours_local = pd.concat([opening_hours_local, temp_df], ignore_index=True)
            opening_hours_local.to_csv('opening_hours_local.csv')
    conn.close()
            
            

    with psycopg2.connect(**prod_params) as conn:
        with conn.cursor() as cursor:
            for restaurant in restaurants:
                local_query_opening_hours = '''
                SELECT oh.start_hour, oh.end_hour, oh.start_date, oh.end_date, ar.name, oh.day_of_week
                FROM public.accounts_openinghours oh
                JOIN public.accounts_restaurant ar 
                ON ar.id = oh.restaurant_id
                WHERE ar.name = %s
                '''
                cursor.execute(local_query_opening_hours, (restaurant,))
                rows = cursor.fetchall()
                # Create a temporary dataframe for each restaurant's data
                temp_df = pd.DataFrame(rows, columns=['start_hour', 'end_hour', 'start_date', 'end_date', 'restaurant', 'day_of_week'])
                
                # Concatenate to the main dataframe
                opening_hours_prod = pd.concat([opening_hours_prod, temp_df], ignore_index=True)
            
            opening_hours_prod.to_csv('opening_hours_prod.csv')
    conn.close()

    df_diff = opening_hours_prod[~opening_hours_prod.apply(tuple,1).isin(opening_hours_local.apply(tuple,1))]
    df_diff.to_csv("differences.csv")


                

            