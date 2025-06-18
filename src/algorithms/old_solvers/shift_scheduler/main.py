from config import SETTINGS, SOLVERS
from solve_salsa import solve_salsa
from solve_alcampo import solve_alcampo

def main():
    solver_key = SETTINGS["solver"]
    solver_config = SOLVERS[solver_key]

    data_sources = solver_config["data_sources"]
    calendario = data_sources["calendario"]
    estimativas = data_sources["estimativas"]
    colaboradores = data_sources["colaboradores"]



    if solver_key == "salsa":
        result = solve_salsa(calendario, estimativas, colaboradores, solver_config)
    elif solver_key == "alcampo":
        result = solve_alcampo(calendario, estimativas, colaboradores, solver_config)
    else:
        raise ValueError(f"Unknown solver type: {solver_key}")

    print("✅ Scheduling complete! Sample output:")
    print(result.head())

if __name__ == "__main__":
    main()
