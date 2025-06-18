from ortools.sat.python import cp_model

from data.read_alcampos import read_data
from model.variables import decision_variables
from model.constraints.alcampo_constraints import (shift_day_constraint, week_working_days_constraint, maximum_continuous_working_days,
    maximum_continuous_working_special_days, maximum_free_days, free_days_special_days, tc_atribution, working_days_special_days, 
    LQ_attribution, LD_attribution, closed_holiday_attribution, holiday_missing_day_attribution, assign_week_shift,
    special_day_shifts ,working_day_shifts, free_day_next_2c, no_free__days_close, space_LQs, day2_quality_weekend, compensation_days, prio_2_3_workers,
    limits_LDs_week, one_free_day_weekly,maxi_free_days_c3d, maxi_LQ_days_c3d, assigns_solution_days, day3_quality_weekend)
from model.optimization.optimization_alcampos import optimization_prediction
from solver.solver import solve

def solve_alcampo(matriz_calendario, matriz_estimativas, matriz_colaborador, config):

    data = read_data(matriz_calendario, matriz_estimativas, matriz_colaborador)

    # return cal, days_of_year, sundays, holidays, special_days, closed_holidays, empty_days, worker_holiday, missing_days, working_days , non_holidays, start_weekday, week_to_days, worker_week_shift, \
    #        colaboradores, workers, contract_type, total_l, total_l_dom, c2d, c3d, l_d, l_q, cxx, t_lq, \
    #        estim, pessObj, min_workers, max_workers

    cal = data[0] 
    days_of_year = data[1]
    sundays = data[2]
    holidays = data[3]
    special_days = data[4]
    closed_holidays = data[5]
    empty_days = data[6]
    worker_holiday = data[7]
    missing_days = data[8]
    working_days = data[9]
    non_holidays = data[10]
    start_weekday = data[11]
    week_to_days = data[12]
    worker_week_shift = data[13]
    colaboradores = data[14]    
    workers = data[15]
    contract_type = data[16]
    total_l = data[17]
    total_l_dom = data[18]
    c2d = data[19]
    c3d = data[20]
    l_d = data[21]
    l_q = data[22]
    cxx = data[23]
    t_lq = data[24]
    tc = data[25]
    df = data[26]
    pessObj = data[27]
    min_workers = data[28]
    max_workers = data[29]
    working_shift_2 = data[30]
    

    model = cp_model.CpModel()
    shifts = config["shifts"]
    check_shift = config["check_shifts"]
    check_shift_special = config["check_shift_special"]
    working_shift = config["working_shifts"]
    max_continuous_days = config["max_continuous_working_days"]

    # Create decision variables
    shift = decision_variables(model, days_of_year, workers, shifts)
    # Constraints
    # Constraint for workers having an assigned shift for each day
    shift_day_constraint(model, shift, days_of_year, workers, shifts)
    
    # Constraint to limit working days in a week based on contract type
    week_working_days_constraint(model, shift, week_to_days, workers, working_shift_2, contract_type)
    
    # Constraint to limit maximum continuous working days to 10
    maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_continuous_days)
    
    # Constraint to limit maximum continuous working special days (Sundays and holidays) to 3
    maximum_continuous_working_special_days(model, shift, special_days, workers, working_shift, contract_type)

    # Constraint to limit maximum free days in a year
    maximum_free_days(model, shift, days_of_year, workers, total_l, c3d)
    
    # Constraint for free days on special days (Sundays and holidays) based on contract
    free_days_special_days(model, shift, special_days, workers, working_days, total_l_dom)

    tc_atribution(model, shift, workers, days_of_year, tc, special_days, working_days)

    working_days_special_days(model, shift, special_days, workers, working_days, l_d, contract_type)
    
    # Constraint for LQ days attribution based on contract
    LQ_attribution(model, shift, workers, working_days, l_q, c2d)
    
    # Constraint for LD days attribution based on contract
    LD_attribution(model, shift, workers, working_days, l_d)
    
    # Constraint for closed holidays attribution
    closed_holiday_attribution(model, shift, workers, closed_holidays)
    
    # Constraint for holiday, missing days and empty days attribution
    holiday_missing_day_attribution(model, shift, workers, worker_holiday, missing_days, empty_days)
    
    # Constraint for worker week shift assignments
    assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift)
    
    # Constraint for working day shifts (only valid shifts on working days)
    working_day_shifts(model, shift, workers, working_days, check_shift)

    # Constraint for special day shifts (only valid shifts on special days)
    special_day_shifts(model, shift, workers, special_days, check_shift_special, working_days)
    
    # Constraint for maximum workers per shift/day
    #maxi_workers(model, shift, non_holidays, workers, working_shift, max_workers)
    
    # Constraint for free days adjacent to weekends
    free_day_next_2c(model, shift, workers, working_days,start_weekday, closed_holidays)
    
    # Constraint to limit consecutive free days during the week
    no_free__days_close(model, shift, workers, working_days, start_weekday, week_to_days, cxx, contract_type, closed_holidays, days_of_year)
    
    # Constraint for day2 quality weekends (free Saturday and Sunday)
    day2_quality_weekend(model, shift, workers, working_days, sundays, c2d, contract_type, closed_holidays)

    space_LQs(model, shift, workers, working_days, t_lq, cal)

    prio_2_3_workers(model, shift, workers, working_days, special_days, start_weekday, week_to_days, contract_type, working_shift)
    
    # Constraint for compensation days for working on special days
    compensation_days(model, shift, workers, working_days, special_days, start_weekday, week_to_days, contract_type, working_shift)
    
    # # Constraint to limit LDs per week based on special days
    limits_LDs_week(model, shift, week_to_days, workers, special_days)

    one_free_day_weekly(model, shift, week_to_days, workers, working_days, contract_type, closed_holidays)
    
    # Set up optimization objective
    optimization_prediction(model,days_of_year, workers, working_shift, shift, pessObj, min_workers, closed_holidays, week_to_days,  working_days, contract_type)

    # Solve the model
    schedule_df = solve(model, days_of_year, workers, special_days, shift, shifts)


    new_model = cp_model.CpModel()


    # Create decision variables
    new_shift = decision_variables(new_model, days_of_year, workers, shifts)
 
    # Constraints
    # Constraint for workers having an assigned shift for each day
    shift_day_constraint(new_model, new_shift, days_of_year, workers, shifts)

    # Constraint for maximum free days in a year
    maxi_free_days_c3d(new_model, new_shift, workers, days_of_year, total_l)

    # Constraint for maximum LQ days in a year
    maxi_LQ_days_c3d(new_model, new_shift, workers, working_days, l_q, c2d, c3d)

    # Assign solution days based on the previous schedule
    assigns_solution_days(new_model, new_shift, workers, days_of_year, schedule_df, working_days, start_weekday, shifts)

    # Constraint for 3-day quality weekends
    day3_quality_weekend(new_model, new_shift, workers, working_days, start_weekday, schedule_df, c3d, contract_type, closed_holidays)

    optimization_prediction(model,days_of_year, workers, working_shift, shift, pessObj, min_workers, closed_holidays, week_to_days,  working_days, contract_type)

    space_LQs(model, shift, workers, working_days, t_lq, cal)

    # Solve the model
    schedule_df = solve(new_model, days_of_year, workers, special_days, new_shift, shifts)


    return schedule_df

