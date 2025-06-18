from ortools.sat.python import cp_model

from data.read_salsa import read_data_salsa
from model.variables import decision_variables
from model.constraints.salsa_constraints import (shift_day_constraint, week_working_days_constraint, maximum_continuous_working_days, maximum_free_days, 
                                                 LD_attribution, closed_holiday_attribution, free_days_special_days_salsa, assign_week_shift, working_day_shifts, 
                                                 salsa_2_consecutive_free_days, salsa_2_day_quality_weekend, salsa_saturday_L_constraint, salsa_week_cut_contraint)
from model.optimization.optimization_salsa import salsa_optimization
from solver.solver import solve

def solve_salsa(matriz_calendario, matriz_estimativas, matriz_colaborador, config):
    # Call the main function to solve the scheduling problem
    data = read_data_salsa(matriz_calendario, matriz_estimativas, matriz_colaborador)

    settings = config["settings"]
    F_special_day = settings["F_special_day"]
    free_sundays_plus_c2d = settings["free_sundays_plus_c2d"]
    missing_days_afect_free_days = settings["missing_days_afect_free_days"]

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
    df = data[25]
    pessObj = data[26]
    min_workers = data[27]
    max_workers = data[28]
    week_to_days_salsa = data[29]
    week_cut = data[30]
    

    model = cp_model.CpModel()
    shifts = config["shifts"]
    check_shift = config["check_shifts"]
    working_shift = config["working_shifts"]
    max_continuous_days = config["max_continuous_working_days"]

    shift = decision_variables(model, days_of_year, workers, shifts)

    shift_day_constraint(model, shift, days_of_year, workers, shifts)

    week_working_days_constraint(model, shift, week_to_days_salsa, workers, working_shift, contract_type)

    maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_continuous_days)

    maximum_free_days(model, shift, days_of_year, workers, total_l, c3d)

    # Constraint for LD days attribution based on contract
    LD_attribution(model, shift, workers, working_days, l_d)

    # Constraint for closed holidays attribution
    closed_holiday_attribution(model, shift, workers, closed_holidays)

    free_days_special_days_salsa(model, shift, special_days, workers, working_days, total_l_dom, free_sundays_plus_c2d, c2d)

    # Constraint for worker week shift assignments
    assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift)
    
    # Constraint for working day shifts (only valid shifts on working days)
    working_day_shifts(model, shift, workers, working_days, check_shift)

    #maxi_workers(model, shift, non_holidays, workers, working_shift, max_workers)

    salsa_2_consecutive_free_days(model, shift, workers, working_days)

    salsa_2_day_quality_weekend(model, shift, workers, contract_type, working_days, sundays, c2d, F_special_day, days_of_year, closed_holidays)

    salsa_saturday_L_constraint(model, shift, workers, working_days, start_weekday)

    salsa_week_cut_contraint(model, shift, workers, week_to_days_salsa, week_cut , start_weekday)

    #salsa_2_free_days_week(model, shift, workers, week_to_days_salsa, working_days)

    salsa_optimization(model, days_of_year, workers, working_shift, shift, pessObj, working_days, closed_holidays, min_workers, week_to_days)

    # Solve the model
    schedule_df = solve(model, days_of_year, workers, special_days, shift, shifts)

    return schedule_df

