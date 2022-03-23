import numpy as np
from scipy.special import gamma
from scipy.special import gammaln
import pandas as pd

def dens_poisson_gamma(x_p, X, N):
    '''
    This function returns the probability that the random variable takes a 
    value x, for a Poisson Gamma distribution.

    Note: for our purpose:

    x_p = goals / corners, etc. to be modelled.
    X = sample of goals / corners.
    N = sample size.

    Args:
        x_p (int): the value of the random variable.
        X (float): the shape parameter.
        N (float): the scale parameter. 
    '''

    first_factor = np.exp(gammaln(x_p + X)-(gammaln(x_p + 1) + gammaln(X)))
    second_factor = (N/(N+1))**(X)
    third_factor = (1/(N+1))**x_p

    prob = first_factor*second_factor*third_factor

    return prob


def model_team_vars(X_1, X_2 = None, limit_x = 15, alpha_1 = 0.001, beta_1 = 0.01):
    '''
    This function returns the probability that a random variable X_1 is greater
    or equal than another random variable X_2. Assuming that both of these 
    follow a Poisson Gamma distribution.

    Args:
        X_1 (list): the sample collected for the X_1 r.v.
        X_2 (list): the sample collected for the X_2 r.v.
    '''

    if X_2 is not None:
      size = len(X_1) + len(X_2)
      size = size + (len(X_1) + len(X_2))*beta_1
      X_1 = X_1.sum() + X_2.sum() + alpha_1*(len(X_1) + len(X_2))*beta_1
    else:
      size = len(X_1)
      size = size + len(X_1)*beta_1
      X_1 = X_1.sum() + alpha_1*len(X_1)*beta_1   

    # Model to a maximum of 15 occurances
    X1_probs = []

    # Probability of occuring i times for each team
    for i in range(0, limit_x):
        X1_probs.append(dens_poisson_gamma(i, X_1, size))

    result_probs = pd.DataFrame({"X1_probs": X1_probs})
    
    return result_probs


def model_match_vars(X_1, X_2):
    '''
    This function returns the probability that a random variable X_1 is greater
    or equal than another random variable X_2. Assuming that both of these are
    independent and follow a Poisson Gamma distribution.

    Args:
        X_1 (list): is the probability distribution of the X_1 r.v.
        X_2 (list): is the probability distribution the X_2 r.v.
    '''

    result_matrix = np.zeros((X_1.shape[0], X_2.shape[0]))

    # Probability of the score being h - a
    for h_index, h in enumerate(X_1):
        for a_index, a in enumerate(X_2):
            result_matrix[h_index, a_index] = h*a

    home_win_prob = 0
    away_win_porb = 0
    tie_prob = 0

    # Win and tie probabilities

    for i in range(X_1.shape[0]):
        for j in range(X_1.shape[0]):
            if i > j:
                home_win_prob += result_matrix[i][j]
            elif i < j:
                away_win_porb += result_matrix[i][j]
            else:
                tie_prob += result_matrix[i][j]

    model_dic = {
        "result_matrix": result_matrix,
        "home_win_prob": home_win_prob,
        "away_win_prob": away_win_porb,
        "tie_prob": tie_prob
    }

    return model_dic