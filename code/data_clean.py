import pandas as pd
import sqlalchemy as sa
from functools import reduce


# NOTE: all data read here is drawn from a 5.5 GB pudl.sqlite file, which I am not uploading to GitHub for space reasons.
# It is available at https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/pudl.sqlite

## electric_energy_dispositions_ferc1
electric_energy_dispositions_ferc1 = pd.read_sql_table('core_ferc1__yearly_energy_dispositions_sched401','sqlite:///pudl.sqlite')
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

electric_operating_expenses_ferc1 = pd.read_sql('out_ferc1__yearly_operating_expenses_sched320','sqlite:///pudl.sqlite')
electric_operating_expenses_ferc1.drop(labels=['record_id','row_type_xbrl'],axis='columns',inplace=True)
electric_operating_expenses_ferc1_wide = pd.pivot(electric_operating_expenses_ferc1,
                                                  index=['utility_id_ferc1','report_year'],
                                                  columns='expense_type',
                                                  values=['dollar_value'])

electric_operating_expenses_ferc1_wide.columns = electric_operating_expenses_ferc1_wide.columns.to_series().str.join('_')
electric_operating_expenses_ferc1_wide.reset_index(inplace=True)
electric_operating_expenses_ferc1_wide.columns = electric_operating_expenses_ferc1_wide.columns.str.replace('dollar_value_', '')

electric_operating_expenses_ferc1_wide = electric_operating_expenses_ferc1_wide[['load_dispatching',
                                                                                 'purchased_power',
                                                                                 'power_production_expenses',
                                                                                 'power_production_expenses_hydraulic_power',
                                                                                 'power_production_expenses_nuclear_power',
                                                                                 'power_production_expenses_other_power',
                                                                                 'power_production_expenses_steam_power',
                                                                                 'administrative_and_general_expenses',
                                                                                 'administrative_and_general_operation_expense',
                                                                                 'administrative_and_general_salaries',
                                                                                 'distribution_expenses',
                                                                                 'distribution_maintenance_expense_electric',
                                                                                 'maintenance_of_line_transformers',
                                                                                'maintenance_of_meters',
                                                                                'maintenance_of_miscellaneous_distribution_plant',
                                                                                'maintenance_of_overhead_lines',
                                                                                'maintenance_of_station_equipment',
                                                                                'maintenance_of_structures_distribution_expense',
                                                                                'maintenance_of_underground_lines',
                                                                                'maintenance_supervision_and_engineering',
                                                                                'maintenance_of_street_lighting_and_signal_systems',
                                                                                'operation_supervision_and_engineering_distribution_expense',
                                                                                'load_dispatching',
                                                                                'station_expenses_distribution',
                                                                                'overhead_line_expenses',
                                                                                'underground_line_expenses',
                                                                                'street_lighting_and_signal_system_expenses',
                                                                                'meter_expenses',
                                                                                'customer_installations_expenses',
                                                                                'miscellaneous_distribution_expenses',
                                                                                'rents_distribution_expense',
                                                                                 'distribution_operation_expenses_electric',
                                                                                 'transmission_expenses',
                                                                                 'regional_market_expenses',
                                                                                 'sales_expenses',
                                                                                 'administrative_and_general_expenses',
                                                                                 'injuries_and_damages',
                                                                                 'customer_account_expenses',
                                                                                 'customer_service_and_information_expenses',
                                                                                 'transmission_maintenance_expense_electric',
                                                                                 'transmission_operation_expense',
                                                                                 'purchased_power',
                                                                                 'power_production_expenses',
                                                                                 'utility_id_ferc1',
                                                                                 'generation_interconnection_studies',
'load_dispatch_monitor_and_operate_transmission_system',
'overhead_line_expense',
'load_dispatch_reliability',
'load_dispatch_transmission_service_and_scheduling',
'maintenance_of_overhead_lines_transmission',
'miscellaneous_transmission_expenses',
'operation_supervision_and_engineering_electric_transmission_expenses',
'overhead_line_expense',
'reliability_planning_and_standards_development',
'reliability_planning_and_standards_development_services',
'rents_transmission_electric_expense',
'scheduling_system_control_and_dispatch_services',
'station_expenses_transmission_expense',
'transmission_of_electricity_by_others',
'transmission_service_studies',
'underground_line_expenses_transmission_expense',
'maintenance_of_communication_equipment_electric_transmission',
'maintenance_of_computer_hardware_transmission',
'maintenance_of_computer_software_transmission',
'maintenance_of_miscellaneous_regional_transmission_plant',
'maintenance_of_miscellaneous_transmission_plant',
'maintenance_of_overhead_lines_transmission',
'maintenance_of_structures_transmission_expense',
'maintenance_of_underground_lines_transmission',
'maintenance_supervision_and_engineering_electric_transmission_expenses',
                                                                                 'report_year']]


# electric_operating_revenues_ferc1

electric_operating_revenues_ferc1 = pd.read_sql('out_ferc1__yearly_operating_revenues_sched300','sqlite:///pudl.sqlite')
electric_operating_revenues_ferc1.drop(labels=['record_id',
                                           'ferc_account',
                                           'row_type_xbrl'
                                           ],axis='columns',inplace=True)
electric_operating_revenues_ferc1_wide = pd.pivot(electric_operating_revenues_ferc1,
                                                  index=['utility_id_ferc1','report_year'],
                                                  columns='revenue_type',
                                                  values=['dollar_value'])
electric_operating_revenues_ferc1_wide.columns = electric_operating_revenues_ferc1_wide.columns.to_series().str.join('_')
electric_operating_revenues_ferc1_wide.reset_index(inplace=True)
electric_operating_revenues_ferc1_wide.columns = electric_operating_revenues_ferc1_wide.columns.str.replace('dollar_value_', '')

electric_operating_revenues_ferc1_wide = electric_operating_revenues_ferc1_wide[[
                                                                                 'sales_for_resale',
                                                                                 'residential_sales',
                                                                                 'sales_to_ultimate_consumers',
                                                                                 'sales_to_railroads_and_railways',
                                                                                 'public_street_and_highway_lighting',
                                                                                 'other_sales_to_public_authorities',
                                                                                 'miscellaneous_service_revenues',
                                                                                 'small_or_commercial',
                                                                                 'electric_operating_revenues',
                                                                                 'other_electric_revenue',
                                                                                 'other_operating_revenues',
                                                                                 'large_or_industrial',
                                                                                 'forfeited_discounts',
                                                                                 'sales_of_electricity',
                                                                                 'utility_id_ferc1',
                                                                                 'revenues_from_transmission_of_electricity_of_others',
                                                                                 'report_year']]



## electricity_sales_by_rate_schedule_ferc1
## Not possible to get subtotals by res/ind/com - data is far too messy. 
## https://data.catalyst.coop/pudl/electricity_sales_by_rate_schedule_ferc1

electricity_sales_by_rate_schedule_ferc1 = pd.read_sql('out_ferc1__yearly_sales_by_rate_schedules_sched304','sqlite:///pudl.sqlite')
electricity_sales_by_rate_schedule_ferc1 = electricity_sales_by_rate_schedule_ferc1[['utility_id_ferc1',
                                                                                     'avg_customers_per_month',
                                                                                     'report_year',
                                                                                     'sales_mwh']]
electricity_sales_by_rate_schedule_ferc1_agg = electricity_sales_by_rate_schedule_ferc1.groupby(['utility_id_ferc1',
                                                  'report_year']).sum()

                                                
# plant in service

plant_in_service =pd.read_sql('out_ferc1__yearly_plant_in_service_sched204','sqlite:///pudl.sqlite')
plant_in_service.drop(['row_type_xbrl','record_id','ferc_account','retirements','adjustments','transfers','plant_status','utility_type'],axis='columns',inplace=True)

generation_plant = plant_in_service[plant_in_service['ferc_account_label'].str.contains("production_plant")]

generation_plant_wide = pd.pivot(generation_plant,
                                                  index=['utility_id_ferc1','report_year'],
                                                  columns='ferc_account_label',
                                                  values=['additions','starting_balance','ending_balance'])
generation_plant_wide.columns = generation_plant_wide.columns.to_series().str.join('_')
generation_plant_wide.reset_index(inplace=True)


transmission_plant = plant_in_service[plant_in_service['ferc_account_label'].str.contains("transmission_plant")]

transmission_plant_wide = pd.pivot(transmission_plant,
                                                  index=['utility_id_ferc1','report_year'],
                                                  columns='ferc_account_label',
                                                  values=['additions','starting_balance','ending_balance'])
transmission_plant_wide.columns = transmission_plant_wide.columns.to_series().str.join('_')
transmission_plant_wide.reset_index(inplace=True)

distribution_plant = plant_in_service[plant_in_service['ferc_account_label'].str.contains("distribution_plant")]

distribution_plant_wide = pd.pivot(distribution_plant,
                                                  index=['utility_id_ferc1','report_year'],
                                                  columns='ferc_account_label',
                                                  values=['additions','starting_balance','ending_balance'])
distribution_plant_wide.columns = distribution_plant_wide.columns.to_series().str.join('_')
distribution_plant_wide.reset_index(inplace=True)

transmission_distribution_plant_wide = pd.merge(transmission_plant_wide,
                                                            distribution_plant_wide,
                                                            on=['utility_id_ferc1','report_year'],
                                  how='outer')

gen_transmission_distribution_plant_wide = pd.merge(generation_plant_wide,
                                                            transmission_distribution_plant_wide,
                                                            on=['utility_id_ferc1','report_year'],
                                  how='outer')

# transmission_statistics_ferc1

transmission_statistics_ferc1 = pd.read_sql('out_ferc1__yearly_transmission_lines_sched422','sqlite:///pudl.sqlite')
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

ferc1_eia_crosswalk = pd.read_csv('../datafiles/utility_id_pudl.csv')
utility_data_nerc_eia861 = pd.read_sql_table('core_eia861__yearly_utility_data_nerc','sqlite:///pudl.sqlite')

nerc_data_onferc1 = pd.merge(ferc1_eia_crosswalk,
                             utility_data_nerc_eia861,
                             how='outer',
                             on='utility_id_eia')
nerc_data_onferc1['report_year'] = nerc_data_onferc1['report_date'].dt.year  

nerc_data_onferc1.drop(labels=['data_maturity','utility_id_pudl','report_date'],axis='columns',inplace=True)
nerc_data_onferc1.to_csv('../datafiles/NERC_merge.csv')


# Merging

dispositions_and_opex = pd.merge(electric_energy_dispositions_ferc1_wide,
                                  electric_operating_expenses_ferc1_wide,
                                  on=['utility_id_ferc1','report_year'],
                                  how='outer')

dispositions_and_opex_and_transmission = pd.merge(dispositions_and_opex,
                                           transmission_statistics_ferc1,
                                           on=['utility_id_ferc1','report_year'],
                                  how='outer')


dispositions_and_opex_and_transmission_and_sales = pd.merge(dispositions_and_opex_and_transmission,
                                                            electricity_sales_by_rate_schedule_ferc1_agg,
                                                            on=['utility_id_ferc1','report_year'],
                                  how='outer')


dispositions_and_opex_and_transmission_and_sales_and_rev = pd.merge(dispositions_and_opex_and_transmission_and_sales,
                                                            electric_operating_revenues_ferc1_wide,
                                                            on=['utility_id_ferc1','report_year'],
                                  how='outer')


dispositions_and_opex_and_transmission_and_sales_and_rev_and_gtdplant = pd.merge(dispositions_and_opex_and_transmission_and_sales_and_rev,
                                                            gen_transmission_distribution_plant_wide,
                                                            on=['utility_id_ferc1','report_year'],
                                  how='outer')

dispositions_and_opex_and_transmission_nerc = pd.merge(
    dispositions_and_opex_and_transmission,
    nerc_data_onferc1,
    how='left',
    on=['utility_id_ferc1','report_year']
)



#dispositions_and_opex_and_transmission.to_csv('dispositions_and_opex_and_transmission.csv')
dispositions_and_opex_and_transmission_and_sales_and_rev.to_csv('../datafiles/dispositions_and_opex_and_transmission_and_sales_and_rev.csv')
dispositions_and_opex_and_transmission_and_sales_and_rev_and_gtdplant.to_csv('../datafiles/dispositions_and_opex_and_transmission_and_sales_and_rev_and_gtdplant.csv')

'''

#NOT useful
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


#utilities_ferc1 = pd.read_sql_table('utilities_ferc1','sqlite:///pudl.sqlite')
#utilities_eia = pd.read_sql_table('utilities_eia','sqlite:///pudl.sqlite')
#eia_pudl_ferc_ids = pd.merge(utilities_ferc1,utilities_eia,how='inner',on='utility_id_pudl')
#eia_nerc_ids = pd.merge(eia_pudl_ferc_ids,
#                        utility_data_nerc_eia861,
#                        how='inner',
#                        on='utility_id_eia')
#eia_nerc_ids['report_year'] = eia_nerc_ids['report_date'].dt.year  
#eia_nerc_ids.drop(labels=['data_maturity','utility_id_pudl','utility_id_eia','report_date'],axis='columns',inplace=True)

#eia_nerc_ids = eia_nerc_ids.groupby(['utility_id_ferc1','utility_name_ferc1','report_year']).agg({'nerc_region': lambda x: list(set(x)),
#                                                                                          'nerc_regions_of_operation':lambda x: list(set(x)),
#                                                                                          'state':lambda x: list(set(x)),
#                                                                                          'utility_name_eia':lambda x: list(set(x)),
#                                                                                          }).reset_index()


'''