    
import logging
import psycopg2
from AlertFunction.params import params
from AlertFunction.constant import restaurants
import pandas as pd

def opening_hours_diff(): 
    # columns = ['start_hour', 'end_hour', 'start_date', 'end_date', 'restaurant', 'day_of_week']
    opening_hours_local = pd.DataFrame(columns=['start_hour', 'end_hour', 'start_date', 'end_date', 'restaurant', 'day_of_week'])
    opening_hours_prod = pd.DataFrame(columns=['start_hour', 'end_hour', 'start_date', 'end_date', 'restaurant', 'day_of_week'])

    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            for restaurant in restaurants:
                local_query_opening_hours = '''
                SELECT oh.start_hour, oh.end_hour, oh.start_date, oh.end_date, ar.name, oh.day_of_week
                FROM public.account_openinghours_unchanged oh
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

            # opening_hours_local.to_csv('opening_hours_local.csv')
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
            
            # opening_hours_prod.to_csv('opening_hours_prod.csv')
    conn.close()

    df_diff = opening_hours_prod[~opening_hours_prod.apply(tuple,1).isin(opening_hours_local.apply(tuple,1))]
    df_diff.reset_index(drop=True, inplace=True)
    # df_diff.to_csv("differences.csv")
    
    return df_diff

def prediction_difference():
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            all_data = pd.DataFrame()
            for restaurant in restaurants:
                highest_percentage_diff_query = '''
                select date,total_gross,created_at, restaurant, percentage_difference from public."Predictions_predictions" 
                where (percentage_difference > 50  or percentage_difference < -50) and restaurant= %s and date(created_at) = CURRENT_DATE
                '''

                cursor.execute(highest_percentage_diff_query,[restaurant,])
                rows = cursor.fetchall()
                temp_df = pd.DataFrame(rows, columns=['date', 'total_gross', 'created_at', 'restaurant', 'percentage_difference'])
                all_data= pd.concat([all_data,temp_df],ignore_index= True)
    conn.close()
    all_data.reset_index(drop=True, inplace=True)
    return all_data

def prediction_restaurant_count():
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            pred_execution = '''
                        WITH MaxDates_daily AS (
                            SELECT restaurant, MAX(created_at) AS max_created_at_daily
                            FROM public."Predictions_predictions"
                            WHERE restaurant NOT IN ('Krunch Oslo', 'Krunch Stavanger')
                            GROUP BY restaurant
                        ),
                        MaxDates_hourly AS (
                            SELECT restaurant, MAX(created_at) AS max_created_at_hourly
                            FROM public."Predictions_predictionsbyhour"
                            WHERE restaurant NOT IN ('Krunch Oslo', 'Krunch Stavanger')
                            GROUP BY restaurant
                        ),
                        MaxDates_supergroup AS (
                            SELECT restaurant, MAX(created_at) AS max_created_at_supergroup
                            FROM public."Predictions_supergroupprediction"
                            WHERE restaurant NOT IN ('Krunch Oslo', 'Krunch Stavanger')
                            GROUP BY restaurant
                        ),
                        MaxDates_group AS (
                            SELECT restaurant, MAX(created_at) AS max_created_at_group
                            FROM public."Predictions_productmixprediction"
                            WHERE restaurant NOT IN ('Krunch Oslo', 'Krunch Stavanger')
                            GROUP BY restaurant
                        ),
                        MaxDates_type AS (
                            SELECT restaurant, MAX(created_at) AS max_created_at_type
                            FROM public."Predictions_typeprediction"
                            WHERE restaurant NOT IN ('Krunch Oslo', 'Krunch Stavanger')
                            GROUP BY restaurant
                        )
                        SELECT 
                            COALESCE(d.restaurant, h.restaurant, s.restaurant, g.restaurant, t.restaurant) AS restaurant,
                            DATE(d.max_created_at_daily) AS daily,
                            DATE(h.max_created_at_hourly) AS hourly,
                            DATE(s.max_created_at_supergroup) AS supergroup,
                            DATE(g.max_created_at_group) AS product_mix_group,
                            DATE(t.max_created_at_type) AS type
                        FROM 
                            MaxDates_daily d
                        FULL OUTER JOIN 
                            MaxDates_hourly h ON d.restaurant = h.restaurant
                        FULL OUTER JOIN 
                            MaxDates_supergroup s ON COALESCE(d.restaurant, h.restaurant) = s.restaurant
                        FULL OUTER JOIN 
                            MaxDates_group g ON COALESCE(d.restaurant, h.restaurant, s.restaurant) = g.restaurant
                        FULL OUTER JOIN 
                            MaxDates_type t ON COALESCE(d.restaurant, h.restaurant, s.restaurant, g.restaurant) = t.restaurant
                        ORDER BY 
                            COALESCE(d.max_created_at_daily, h.max_created_at_hourly, s.max_created_at_supergroup, g.max_created_at_group, t.max_created_at_type) DESC;
            '''
            cursor.execute(pred_execution)
            rows = cursor.fetchall()
            temp_df = pd.DataFrame(rows, columns=['restaurant','Daily','Hourly','Supergroup','Group','Type'])
            temp_df.insert(0, 's.n', range(1, len(temp_df) + 1))
            temp_df.reset_index(drop=True, inplace=True)
    conn.close()
    return temp_df
