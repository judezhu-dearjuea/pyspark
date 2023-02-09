# Databricks notebook source
def frequency_recency_summary(df,customer_id_col, transaction_col, observation_period_end=None, freq="D"):
  id_col = customer_id_col
  trans_col = transaction_col
  if observation_period_end == None:
    cur_date = df.select(F.max(trans_col)).toPandas().iloc[0,0]
  else:
    cur_date = datetime.datetime.strptime(observation_period_end, "%Y-%m-%d").date()
  
  if freq == "D":
    period = "day"
    period_length = 1
  elif freq == "M":
    period = "month"
    period_length = 30
  elif freq == "Y":
    period = "year"
    period_length = 365
    
  base_df = (df
          .filter(F.col(trans_col) <= cur_date)
          .withColumn('trans_period',F.date_trunc(period, F.col(trans_col).cast('date')))
          .groupBy(id_col,'trans_period')
          .agg(F.count(trans_col).alias('no_purchase'))
          .withColumn('first_purchase',F.min('trans_period').over(W.partitionBy(id_col)))
          .withColumn('last_purchase', F.max('trans_period').over(W.partitionBy(id_col)))
          .withColumn('no_transaction', F.count('trans_period').over(W.partitionBy(id_col)))
          .withColumn('frequency', F.col('no_transaction')-F.lit(1))
          .withColumn('row', F.row_number().over(W.partitionBy(id_col).orderBy('trans_period')))
          .filter('row == 1')
                )
  
  period_df = (base_df
          .withColumn('T',F.datediff(F.lit(cur_date),F.col('first_purchase'))/period_length)
          .withColumn('recency',F.datediff(F.col('last_purchase'), F.col('first_purchase'))/period_length)
          .select(id_col,'frequency','recency','T')
                 )
  return period_df
  
def calibration_and_holdout_summary(df, customer_id_col, transaction_col, calibration_period_end, observation_period_end=None, freq="D"):
  id_col = customer_id_col
  trans_col = transaction_col

  if freq == "D":
    period = "day"
    period_length = 1
  elif freq == "M":
    period = "month"
    period_length = 30
  elif freq == "Y":
    period = "year"
    period_length = 365
  

  if observation_period_end == None:
    cur_date = df.select(F.max(trans_col)).toPandas().iloc[0,0]
  else:
    cur_date = datetime.datetime.strptime(observation_period_end, "%Y-%m-%d").date()

  observation_period_date = datetime.datetime.strptime(observation_period_end, "%Y-%m-%d").date()
  calibration_period_date = datetime.datetime.strptime(calibration_period_end, "%Y-%m-%d").date()
  duration_holdout = (observation_period_date - calibration_period_date)

  calibration_summary_df = frequency_recency_summary(
                         df,
                         id_col,
                         trans_col,
                         observation_period_end=calibration_period_end,
                         freq=freq)

  for c in calibration_summary_df.columns[1:]:
    calibration_summary_df = (calibration_summary_df
                                                  .withColumnRenamed(c,c + '_cal')
                                                  .drop('c')
                           )
  if duration_holdout.days <=0:
    raise ValueError('obervation_period_end should be after calibration_period_end')

  hold_out_df = (df.filter((F.col(trans_col)> calibration_period_end) & (F.col(trans_col) <= observation_period_end))
                 .withColumn('trans_period', F.date_trunc(period, F.col(trans_col).cast('date')))
                 .groupBy(id_col,'trans_period')
                 .agg(F.count(trans_col).alias('no_purchase'))
                 .withColumn('frequency_holdout', F.count('trans_period').over(W.partitionBy('unique_account_id')))
                 .withColumn('row', F.row_number().over(W.partitionBy(id_col).orderBy('trans_period')))
                 .filter('row == 1')
                 .select(id_col,'frequency_holdout')
              )
  combined_df = (calibration_summary_df.join(hold_out_df, on = id_col, how = 'left')
                                     .withColumn('duration_holdout',F.lit(duration_holdout.days/period_length))
                                     .fillna(value=0,subset=["frequency_holdout"])
              )
  return combined_df
  
