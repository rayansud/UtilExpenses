import pandas as pd
import sqlalchemy as sa
from functools import reduce

# NOTE: all data read here is drawn from a 5.5 GB pudl.sqlite file, which I am not uploading to GitHub for space reasons.
# It is available at https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/pudl.sqlite

## electric_energy_dispositions_ferc1
electric_energy_dispositions_ferc1 = pd.read_sql('electric_energy_dispositions_ferc1','sqlite:///pudl.sqlite')
electric_energy_dispositions_ferc1.drop(labels=['row_type_xbrl','record_id'],axis='columns',inplace=True)
electric_energy_dispositions_ferc1_wide = pd.pivot(electric_energy_dispositions_ferc1,index=['utility_id_ferc1','report_year'],
                                                   columns='energy_disposition_type',
                                                   values=['energy_mwh'])
electric_energy_dispositions_ferc1_wide.columns = electric_energy_dispositions_ferc1_wide.columns.to_series().str.join('_')
electric_energy_dispositions_ferc1_wide.reset_index(inplace=True)
electric_energy_dispositions_ferc1_wide.columns = electric_energy_dispositions_ferc1_wide.columns.str.replace('energy_mwh_', '')
electric_energy_dispositions_ferc1_wide = electric_energy_dispositions_ferc1_wide[['disposition_of_energy',
                                                                                   'energy_losses',
                                                                                   'internal_use_energy',
                                                                                   'megawatt_hours_sold_sales_to_ultimate_consumers',
                                                                                   'utility_id_ferc1',
                                                                                   'report_year']]

## electric_operating_expenses_ferc1

electric_operating_expenses_ferc1 = pd.read_sql('electric_operating_expenses_ferc1','sqlite:///pudl.sqlite')
electric_operating_expenses_ferc1.drop(labels=['record_id','row_type_xbrl'],axis='columns',inplace=True)
electric_operating_expenses_ferc1_wide = pd.pivot(electric_operating_expenses_ferc1,
                                                  index=['utility_id_ferc1','report_year'],
                                                  columns='expense_type',
                                                  values=['dollar_value'])

electric_operating_expenses_ferc1_wide.columns = electric_operating_expenses_ferc1_wide.columns.to_series().str.join('_')
electric_operating_expenses_ferc1_wide.reset_index(inplace=True)
electric_operating_expenses_ferc1_wide.columns = electric_operating_expenses_ferc1_wide.columns.str.replace('dollar_value_', '')

electric_operating_expenses_ferc1_wide = electric_operating_expenses_ferc1_wide[['load_dispatching',
                                                                                 'administrative_and_general_expenses',
                                                                                 'administrative_and_general_operation_expense',
                                                                                 'administrative_and_general_salaries',
                                                                                 'distribution_expenses',
                                                                                 'distribution_maintenance_expense_electric',
                                                                                 'distribution_operation_expenses_electric',
                                                                                 'transmission_expenses',
                                                                                 'transmission_maintenance_expense_electric',
                                                                                 'transmission_operation_expense',
                                                                                 'underground_line_expenses',
                                                                                 'overhead_line_expenses',
                                                                                 'overhead_line_expense',
                                                                                 'utility_id_ferc1',
                                                                                 'report_year']]





# transmission_statistics_ferc1

transmission_statistics_ferc1 = pd.read_sql('transmission_statistics_ferc1','sqlite:///pudl.sqlite')
transmission_statistics_ferc1.drop(labels=['record_id',
                                           'conductor_size_and_material',
                                           'start_point',
                                           'end_point',
                                           'operating_voltage_kv',
                                           'designed_voltage_kv',
                                           'supporting_structure_type',
                                           'conductor_size_and_material',
                                           ],axis='columns',inplace=True)


transmission_statistics_ferc1 = transmission_statistics_ferc1.groupby(
   ['utility_id_ferc1', 'report_year']
).agg('sum').reset_index()

# utility-nerc crosswalk


utilities_ferc1 = pd.read_sql_table('utilities_ferc1','sqlite:///pudl.sqlite')
utilities_eia = pd.read_sql_table('utilities_eia','sqlite:///pudl.sqlite')
utility_data_nerc_eia861 = pd.read_sql_table('utility_data_nerc_eia861','sqlite:///pudl.sqlite')


eia_pudl_ferc_ids = pd.merge(utilities_ferc1,utilities_eia,how='inner',on='utility_id_pudl')
eia_nerc_ids = pd.merge(eia_pudl_ferc_ids,
                        utility_data_nerc_eia861,
                        how='inner',
                        on='utility_id_eia')
eia_nerc_ids['report_year'] = eia_nerc_ids['report_date'].dt.year  
eia_nerc_ids.drop(labels=['data_maturity','utility_id_pudl','utility_id_eia','report_date'],axis='columns',inplace=True)

eia_nerc_ids = eia_nerc_ids.groupby(['utility_id_ferc1','utility_name_ferc1','report_year']).agg({'nerc_region': lambda x: list(set(x)),
                                                                                          'nerc_regions_of_operation':lambda x: list(set(x)),
                                                                                          'state':lambda x: list(set(x)),
                                                                                          'utility_name_eia':lambda x: list(set(x)),
                                                                                          }).reset_index()



# Merging

dispositions_and_opex = pd.merge(electric_energy_dispositions_ferc1_wide,
                                  electric_operating_expenses_ferc1_wide,
                                  on=['utility_id_ferc1','report_year'],
                                  how='outer')

dispositions_and_opex_and_transmission = pd.merge(dispositions_and_opex,
                                           transmission_statistics_ferc1,
                                           on=['utility_id_ferc1','report_year'],
                                  how='outer')
dispositions_and_opex_and_transmission_nerc = pd.merge(
    dispositions_and_opex_and_transmission,
    eia_nerc_ids,
    how='left',
    on=['utility_id_ferc1','report_year']
)

dispositions_and_opex_and_transmission_nerc.to_csv('out.csv')
'''

## electricity_sales_by_rate_schedule_ferc1
## Doesn't work - data is far too messy. Not possible to get subtotals by res/ind/com
## https://data.catalyst.coop/pudl/electricity_sales_by_rate_schedule_ferc1

electricity_sales_by_rate_schedule_ferc1 = pd.read_sql('electricity_sales_by_rate_schedule_ferc1','sqlite:///pudl.sqlite')
electricity_sales_by_rate_schedule_ferc1.drop(labels=['record_id','billing_status'],axis='columns',inplace=True)
electricity_sales_by_rate_schedule_ferc1.rename(columns={'dollar_value':'sales_revenue'},inplace=True)
electricity_sales_by_rate_schedule_ferc1 = electricity_sales_by_rate_schedule_ferc1[electricity_sales_by_rate_schedule_ferc1['rate_schedule_description'].str.contains('total',na=False)]


electricity_sales_by_rate_schedule_ferc1 = electricity_sales_by_rate_schedule_ferc1[electricity_sales_by_rate_schedule_ferc1['rate_schedule_type'].isin(['residential','commercial','industrial'])]
electricity_sales_by_rate_schedule_ferc1= electricity_sales_by_rate_schedule_ferc1.groupby(
   ['utility_id_ferc1', 'report_year','rate_schedule_type']
).agg(
    {
         'kwh_per_customer':'mean',    # Sum duration per group,
         'revenue_per_kwh':'mean',
         'sales_revenue':sum,
         'sales_mwh':sum,
         'avg_customers_per_month':'mean'
    }
).reset_index()
'''