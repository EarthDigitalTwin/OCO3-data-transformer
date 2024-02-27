# Copyright 2023 California Institute of Technology (Caltech)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

FILL_VALUE = -1

TARGET_TYPES = dict(
    fossil=1,       # Numeric
    ecostress=2,    # *** Text ***
    sif=3,          # *** Text ***
    volcano=4,      # Numeric
    tccon=5,        # Numeric
    other=6,        # Mixture; use a best-effort approach to extract numerical ids
    fill=-1
)

ECOSTRESS_IDS = dict(
    afln=1,
    ar_slu=2,
    ar_vir=3,
    au_asm=4,
    au_cum=5,
    au_das=6,
    au_dry=7,
    au_how=8,
    au_lit=9,
    au_stp=10,
    au_tum=11,
    au_wom=12,
    au_ync=13,
    bdog=14,
    be_lon=15,
    be_vie=16,
    br_cmt=17,
    br_no=18,
    ch_dav=19,
    ch_fru=20,
    ch_lae=21,
    cr_fsc=22,
    cr_srnp_emss=23,
    cz_bk1=24,
    de_rus=25,
    de_tha=26,
    fr_fon=27,
    il_yat=28,
    it_cp2=29,
    it_tor=30,
    ke_mak=31,
    kr_gck=32,
    ne_waf=33,
    nz_bfm=34,
    nz_kop=35,
    nz_oxf=36,
    nz_sco=37,
    sleg=38,
    ssh_czo_cal=39,
    ssh_czo_shale=40,
    us_arm=41,
    us_bar=42,
    us_bi1=43,
    us_bsg=44,
    us_ced=45,
    us_cf1=46,
    us_cs1=47,
    us_cz1=48,
    us_cz2=49,
    us_hn1=50,
    us_hn2=51,
    us_kfs=52,
    us_kon=53,
    us_los=54,
    us_me2=55,
    us_men=56,
    us_mms=57,
    us_mrf=58,
    us_ro4=59,
    us_rr=60,
    us_scc=61,
    us_scs=62,
    us_ses=63,
    us_slt=64,
    us_sp=65,
    us_syv=66,
    us_tx2=67,
    us_tx5=68,
    us_tx6=69,
    us_tx9=70,
    us_var=71,
    us_vcm=72,
    us_wjs=73,
    us_wkg=74,
    us_wpp=75,
    us_wwt=76,
)

SIF_IDS = dict(
    atto=1,
    atto_2=2,
    hrv=3,
    jro=4,
    laselva=5,
    mead=6,
    mpj=7,
    mzo=8,
    niwot=9,
    oko=10,
    santarita=11,
    shq=12,
    umb=13,
    uva=14,
)

OTHER_ID_NAN = 0


def extract_id(id_string: str, id_type: int = TARGET_TYPES['other'],) -> int:
    if id_type in [TARGET_TYPES['ecostress'], TARGET_TYPES['sif']]:
        idx = id_string.find('_')

        if idx == -1:
            return OTHER_ID_NAN

        lookup = ECOSTRESS_IDS if id_type == TARGET_TYPES['ecostress'] else SIF_IDS

        return lookup.get(id_string[idx+1:], OTHER_ID_NAN)
    elif id_type == TARGET_TYPES['other']:
        idx = [m.span()[0] for m in re.finditer(r'\d+$', id_string)][:1]

        if len(idx) == 0:
            return OTHER_ID_NAN
        else:
            return int(id_string[idx[0]:])
    else:
        return int(id_string[[m.span()[0] for m in re.finditer(r'\d+$', id_string)][:1][0]:])


def determine_id_type(id_string: str) -> int:
    for type_name in list(TARGET_TYPES.keys())[:-2]:
        if id_string.startswith(type_name):
            return TARGET_TYPES[type_name]

    return TARGET_TYPES['other']
