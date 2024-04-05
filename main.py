import zipfile
import pandas as pd
import random

def find_max_value_days(df):
    with zipfile.ZipFile('data_for_testing.zip', 'r') as zf:
        #считываем данные
        files = zf.namelist()
        variant_files = [file for file in files if file.endswith('.csv')]
        random_variant_file = random.choice(variant_files)
        with zf.open(random_variant_file) as file:
            df = pd.read_csv(file, sep='\t')
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['month'] = df['timestamp'].dt.to_period('M')
        df = df.sort_values(by='timestamp')


        
        first_visit_day = df.groupby('userid')['timestamp'].min().dt.date # 1день первого захода для каждого пользователя
        first_order_day = df[df['action'] == 'confirmation'].groupby('userid')['timestamp'].min().dt.date # 1 день подтверждения заказа для каждого пользователя

        user_days = pd.DataFrame({'first_visit_day': first_visit_day, 'first_order_day': first_order_day}).reset_index()
        filtered_users = user_days[user_days['first_visit_day'] == user_days['first_order_day']]
        valid_userids = filtered_users['userid'].tolist()
        df = df[df['userid'].isin(valid_userids)]
        df = df[df['action'].isin(['confirmation'])]

        df = df.drop_duplicates(subset='userid', keep='first')
        # удаляем пеервый и последний месяц
        first_month = df['timestamp'].dt.to_period('M').min()
        last_month = df['timestamp'].dt.to_period('M').max()
        df = df[(df['timestamp'].dt.to_period('M') != first_month) & (df['timestamp'].dt.to_period('M') != last_month)]

        daily_sums = df.groupby(pd.Grouper(key='timestamp', freq='D'))['value'].sum()
        monthly_max_dates = daily_sums.groupby(daily_sums.index.month).idxmax()

        
        monthly_max_values = daily_sums.loc[monthly_max_dates] # Получение значений для дней с максимальной суммой
        monthly_max_values_with_index = monthly_max_values.reset_index()
        monthly_max_values_with_index.columns = ['timestamp', 'value']
        monthly_max_values_with_index.to_csv('output.csv', sep='\t', index=False)