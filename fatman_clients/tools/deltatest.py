
# List of atomic elements covered in the deltatest
ATOMIC_ELEMENTS = {
    "H":  {"sym": "H",  "num":   1, "name": "hydrogen"},
    "He": {"sym": "He", "num":   2, "name": "helium"},
    "Li": {"sym": "Li", "num":   3, "name": "lithium"},
    "Be": {"sym": "Be", "num":   4, "name": "beryllium"},
    "B":  {"sym": "B",  "num":   5, "name": "boron"},
    "C":  {"sym": "C",  "num":   6, "name": "carbon"},
    "N":  {"sym": "N",  "num":   7, "name": "nitrogen"},
    "O":  {"sym": "O",  "num":   8, "name": "oxygen"},
    "F":  {"sym": "F",  "num":   9, "name": "fluorine"},
    "Ne": {"sym": "Ne", "num":  10, "name": "neon"},
    "Na": {"sym": "Na", "num":  11, "name": "sodium"},
    "Mg": {"sym": "Mg", "num":  12, "name": "magnesium"},
    "Al": {"sym": "Al", "num":  13, "name": "aluminium"},
    "Si": {"sym": "Si", "num":  14, "name": "silicon"},
    "P":  {"sym": "P",  "num":  15, "name": "phosphorus"},
    "S":  {"sym": "S",  "num":  16, "name": "sulfur"},
    "Cl": {"sym": "Cl", "num":  17, "name": "chlorine"},
    "Ar": {"sym": "Ar", "num":  18, "name": "argon"},
    "K":  {"sym": "K",  "num":  19, "name": "potassium"},
    "Ca": {"sym": "Ca", "num":  20, "name": "calcium"},
    "Sc": {"sym": "Sc", "num":  21, "name": "scandium"},
    "Ti": {"sym": "Ti", "num":  22, "name": "titanium"},
    "V":  {"sym": "V",  "num":  23, "name": "vanadium"},
    "Cr": {"sym": "Cr", "num":  24, "name": "chromium"},
    "Mn": {"sym": "Mn", "num":  25, "name": "manganese"},
    "Fe": {"sym": "Fe", "num":  26, "name": "iron"},
    "Co": {"sym": "Co", "num":  27, "name": "cobalt"},
    "Ni": {"sym": "Ni", "num":  28, "name": "nickel"},
    "Cu": {"sym": "Cu", "num":  29, "name": "copper"},
    "Zn": {"sym": "Zn", "num":  30, "name": "zinc"},
    "Ga": {"sym": "Ga", "num":  31, "name": "gallium"},
    "Ge": {"sym": "Ge", "num":  32, "name": "germanium"},
    "As": {"sym": "As", "num":  33, "name": "arsenic"},
    "Se": {"sym": "Se", "num":  34, "name": "selenium"},
    "Br": {"sym": "Br", "num":  35, "name": "bromine"},
    "Kr": {"sym": "Kr", "num":  36, "name": "krypton"},
    "Rb": {"sym": "Rb", "num":  37, "name": "rubidium"},
    "Sr": {"sym": "Sr", "num":  38, "name": "strontium"},
    "Y":  {"sym": "Y",  "num":  39, "name": "yttrium"},
    "Zr": {"sym": "Zr", "num":  40, "name": "zirconium"},
    "Nb": {"sym": "Nb", "num":  41, "name": "niobium"},
    "Mo": {"sym": "Mo", "num":  42, "name": "molybdenum"},
    "Tc": {"sym": "Tc", "num":  43, "name": "technetium"},
    "Ru": {"sym": "Ru", "num":  44, "name": "ruthenium"},
    "Rh": {"sym": "Rh", "num":  45, "name": "rhodium"},
    "Pd": {"sym": "Pd", "num":  46, "name": "palladium"},
    "Ag": {"sym": "Ag", "num":  47, "name": "silver"},
    "Cd": {"sym": "Cd", "num":  48, "name": "cadmium"},
    "In": {"sym": "In", "num":  49, "name": "indium"},
    "Sn": {"sym": "Sn", "num":  50, "name": "tin"},
    "Sb": {"sym": "Sb", "num":  51, "name": "antimony"},
    "Te": {"sym": "Te", "num":  52, "name": "tellurium"},
    "I":  {"sym": "I",  "num":  53, "name": "iodine"},
    "Xe": {"sym": "Xe", "num":  54, "name": "xenon"},
    "Cs": {"sym": "Cs", "num":  55, "name": "caesium"},
    "Ba": {"sym": "Ba", "num":  56, "name": "barium"},
    "Lu": {"sym": "Lu", "num":  71, "name": "lutetium"},
    "Hf": {"sym": "Hf", "num":  72, "name": "hafnium"},
    "Ta": {"sym": "Ta", "num":  73, "name": "tantalum"},
    "W":  {"sym": "W",  "num":  74, "name": "tungsten"},
    "Re": {"sym": "Re", "num":  75, "name": "rhenium"},
    "Os": {"sym": "Os", "num":  76, "name": "osmium"},
    "Ir": {"sym": "Ir", "num":  77, "name": "iridium"},
    "Pt": {"sym": "Pt", "num":  78, "name": "platinum"},
    "Au": {"sym": "Au", "num":  79, "name": "gold"},
    "Hg": {"sym": "Hg", "num":  80, "name": "mercury"},
    "Tl": {"sym": "Tl", "num":  81, "name": "thalium"},
    "Pb": {"sym": "Pb", "num":  82, "name": "lead"},
    "Bi": {"sym": "Bi", "num":  83, "name": "bismuth"},
    "Po": {"sym": "Po", "num":  84, "name": "polonium"},
    "At": {"sym": "At", "num":  85, "name": "astatine"},
    "Rn": {"sym": "Rn", "num":  86, "name": "radon"},
}


NUM2SYM = {e['num']: e['sym'] for e in ATOMIC_ELEMENTS.values()}


def eos(V0, B0, B1, E0=0.):
    import numpy as np

    B0 = B0 * 1e9 / 1.602176565e-19 / 1e30
    rng = np.linspace(0.93*V0, 1.07*V0, 40)
    E = [ E0 + 9./16. * V0 * B0 *  ( ((V0/v)**(2./3.) -1)**3 * B1 +
                                     ((V0/v)**(2./3.) -1)**2 * (6-4*(V0/v)**(2./3.)) ) for v in rng]
    return rng, np.array(E)
