
import periodictable

# List of atomic elements covered in the deltatest
SYM_LIST = [
    "H", "He",
    "Li", "Be", "B", "C", "N", "O", "F", "Ne",
    "Na", "Mg", "Al", "Si", "P", "S", "Cl", "Ar",
    "K", "Ca", "Sc", "Ti", "V", "Cr", "Mn", "Fe", "Co", "Ni", "Cu", "Zn", "Ga", "Ge", "As", "Se", "Br", "Kr",
    "Rb", "Sr", "Y", "Zr", "Nb", "Mo", "Tc", "Ru", "Rh", "Pd", "Ag", "Cd", "In", "Sn", "Sb", "Te", "I", "Xe",
    "Cs", "Ba", "Lu", "Hf", "Ta", "W", "Re", "Os", "Ir", "Pt", "Au", "Hg", "Tl", "Pb", "Bi", "Po", "At", "Rn",
]

ATOMIC_ELEMENTS = {e.symbol: {"sym": e.symbol, "num": e.number}
                   for e in periodictable.elements if e.symbol in SYM_LIST}

NUM2SYM = [e.symbol for e in periodictable.elements]


def eos(V0, B0, B1, E0=0.):
    import numpy as np

    B0 = B0 * 1e9 / 1.602176565e-19 / 1e30
    rng = np.linspace(0.93*V0, 1.07*V0, 40)
    E = [ E0 + 9./16. * V0 * B0 *  ( ((V0/v)**(2./3.) -1)**3 * B1 +
                                     ((V0/v)**(2./3.) -1)**2 * (6-4*(V0/v)**(2./3.)) ) for v in rng]
    return rng, np.array(E)
