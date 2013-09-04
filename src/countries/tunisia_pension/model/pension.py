# -*- coding:utf-8 -*-
# Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul

"""
openFisca, Logiciel libre de simulation du système socio-fiscal français
Copyright © 2011 Clément Schaff, Mahdi Ben Jelloul

This file is part of openFisca.

    openFisca is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    openFisca is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with openFisca.  If not, see <http://www.gnu.org/licenses/>.
"""
from __future__ import division
from numpy import ( maximum as max_, minimum as min_, logical_xor as xor_, logical_and as and_, 
                     logical_not as not_, round) 


###############################################################################
## Pensions
###############################################################################


def _sal_ref(sal0, sal1, sal2, sal3, sal4, sal5, sal6, sal7, sal8, sal9, sal10):
    """
    Salaire de référence
    """
    # TODO: gérer le nombre d'année
    # TODO: plafonner le salaire de référence à 6 fois le smic de l'année d'encaissement
    sal_ref = (sal0 + sal1 + sal2 + sal3 + sal4 + sal5 + sal6 + sal7 + sal8 + sal9 + sal10)/10
    
    return sal_ref

def _pension(nb_trim_val, sal_ref, regime, age, _P):
    """
    Pension
    """
    taux_ann_base = _P.pension.rsna.taux_ann_base
    taux_ann_sup  = _P.pension.rsna.taux_ann_sup 
    duree_stage = _P.pension.rsna.stage_derog
    age_elig = _P.pension.rsna.age_dep_anticip
    periode_remp_base = _P.pension.rsna.periode_remp_base
    plaf_pension = _P.pension.rsna.plaf_taux_pension
    smig = _P.param_gen.smig_48h
    stage = nb_trim_val > 4*duree_stage 
    pension_min = (stage)*_P.pension.rsna.pension_min.sup + not_(stage)*_P.pension.rsna.pension_min.inf  

    elig_age = age > age_elig
    elig = stage*elig_age*(sal_ref>0) 
    taux_pension = ( (nb_trim_val < 4*periode_remp_base)*(nb_trim_val/4*taux_ann_base) +
                       (nb_trim_val >= 4*periode_remp_base)*( taux_ann_base*periode_remp_base + (nb_trim_val/4 - periode_remp_base)*taux_ann_sup )) 
    
    montant = min_(taux_pension, plaf_pension)*sal_ref
    montant_percu = max_(montant, pension_min*smig)
    pension = elig*montant_percu
    
    
    return pension