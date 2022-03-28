import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit



def calculate_K_factor(clientid, inf_pot_interactions, verbose=False, figure=False):
    """Calculate virality K-factor for the given clients, with the condition of
    having at least 5 datapoints (disctint shares) in order to avoid a spurious metric.

    :param clientid: Client Id
    :type clientid: int
    :param inf_pot_interactions: dataframe with interactions of potential influencers
    :type inf_pot_interactions: dataframe
    :param verbose: Flag to print info, defaults to False
    :type verbose: bool, optional
    :param figure: Flag to save figure of fitting, defaults to False
    :type figure: bool, optional
    :return: K-factor for the given client Id. 
    :rtype: float
    """

    # obtener usuario de tabla de interacciones
    usuario = inf_pot_interactions.groupby('ClientId').get_group(clientid)
    usuario.sort_values('OrderUTC', inplace=True)

    # tratamiento para fechas
    tiempo_dias = usuario.sort_values('OrderUTC')#['OrderUTC']
    day_group = tiempo_dias.groupby(tiempo_dias.OrderUTC.dt.date)
    t0 = pd.to_datetime(usuario['OrderUTC'], format="%Y-%m-%d %H:%M:%S").dt.date.min()

    if verbose:
        print()
        print(f'ClientId: {clientid}')
        print('t0', t0)

    X_data = []
    Y_data = []
    y_ = []
    j=0
    for d, dgroup in day_group:        
        X_data.append((d-t0).days) # cuántos días han transcurrido desde el día 0
        y_.append(len(dgroup)) # cuántos usuarios usaron ese cupón ese día
        Y_data.append(sum(y_)) # se guarda numero de usuarios acumulados
        
        if verbose:
            print(f'Fecha: {d} | Usos: {len(dgroup)} | Días transcurridos: {(d-t0).days}')
        
        if j == 0:
            u0 = len(dgroup)
        j+=1

    if verbose:
        print('X: ',X_data)
        print('Y: ',Y_data)
        print('U0: ',u0)
    
    # Función de viralidad
    Ct = 1    # Se van a tomar los ciclos como días
    def virality(t, k):
        return u0 * (k**(t/Ct) -1)/(k-1)
    
    # Aqui verificar que hayan AL MENOS 5 datos, de lo contrario los resultados son espurios
    if len(X_data) >= 5:
        try:
            # Fit
            popt, pcov = curve_fit(virality, X_data, Y_data, bounds=(0.01, 10.), )
            k_fac = popt[0]
            
            if verbose:
                print(f'Ciclo {Ct} [dias] | K factor: {k_fac} | pcov: {pcov[0][0]}')
            if figure:
                plt.scatter(X_data, Y_data)
                plt.plot(X_data, virality(np.array(X_data), *popt), label=f'K={k_fac}')
                plt.title(f'{clientid}')
                plt.xlabel('Días transcurridos desde primer uso')
                plt.ylabel('Cantidad acumulada de usos')
                plt.legend()

            return popt[0]

        except:
            return np.nan
    else:
        if verbose:
            print(f'Sin data suficiente >=5, en cambio {len(X_data)} datos encontrados')
        return np.nan