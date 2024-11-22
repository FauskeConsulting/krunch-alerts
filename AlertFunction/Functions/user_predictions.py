import psycopg2
import pandas as pd
from AlertFunction.params import params
from AlertFunction.constant import restaurants
from datetime import timedelta,datetime


def user_predictions():
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            yesterday_date = datetime.now() - timedelta(days=1)
            from_date = yesterday_date.replace(hour=1,minute=1).strftime("%Y-%m-%d %H:%M:%S")
            all_data = pd.DataFrame()
            for restaurant in restaurants:
                end_date_query = f'''
                    SELECT date, restaurant, total_gross as user_prediction,comment,created_at
                        FROM public."Predictions_userpredictions" 
                        where created_at > '{from_date}' and restaurant = '{restaurant}'
                    '''
                
                cursor.execute(end_date_query)
                rows = cursor.fetchall()
                temp_df = pd.DataFrame(rows, columns=['date','restaurant', 'user_prediction', 'comment','created_at'])
                all_data= pd.concat([all_data,temp_df],ignore_index= True)
    
    all_data.reset_index(drop=True, inplace=True)
    df_sorted = all_data.sort_values(by="date")
    df_sorted.insert(0,'s.n', range(1, len(df_sorted) + 1))
    conn.close()
    return df_sorted