import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

def calc_F_2_tier(F_formula, f_val, c_range):
    return [F_formula(f_val, c * f_val) for c in c_range]

def plot_F_2_tier(f_val, c_range, F_results_sym, F_results_asym):
    assert len(c_range) == len(F_results_sym)
    assert len(c_range) == len(F_results_asym)

    plt.plot(c_range, F_results_sym, label="sym")
    plt.plot(c_range, F_results_asym, label="asym")

    plt.legend()

    plt.title(f"f = {f_val:.3f}")

    plt.xlabel("ratio c = f'/f")
    plt.ylabel("overall unavailability rate F")

    plt.savefig("F_2_tier.png", dpi=120)
    plt.close()

if __name__ == "__main__":
    f_val = 0.01
    c_range = range(1, 16)
    F_sym = lambda f, fp: fp**3 + 6 * f * fp**2 + 3 * f**2 * fp
    F_asym = lambda f, fp: f**2 + 6 * f * fp**2

    F_sym_results = calc_F_2_tier(F_sym, f_val, c_range)
    F_asym_results = calc_F_2_tier(F_asym, f_val, c_range)

    plot_F_2_tier(f_val, c_range, F_sym_results, F_asym_results)
