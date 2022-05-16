library("survival") 

@transform_pandas(
    Output(rid="ri.vector.main.execute.5177e33a-1400-43b5-8919-e5d090f19121"),
    dataset_cats=Input(rid="ri.vector.main.execute.b3924844-f15b-438c-b503-bdcef00b6f3b")
)
library("survival") 
cox_Intx_Rx <- function(dataset_cats) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19*CVD_Rx + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_cats)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.650f2950-2bb7-406b-a5d0-7cbcd46a4fbd"),
    dataset_cats=Input(rid="ri.vector.main.execute.b3924844-f15b-438c-b503-bdcef00b6f3b")
)
library("survival") 
cox_Intx_age <- function(dataset_cats) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19*agecat + age + racegroup + sex + hxDM + hxCAD + hxObese + hxCKD + hxCOPD +                 hxHTN + ACEi + ARB + BB + statin, data=dataset_cats)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.0012774e-cad6-4e4e-8fcd-c25e2b642d68"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
library("survival") 
cox_Intx_race <- function(dataset_all_Rx) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19*racegroup + age + sex +  hxDM + hxCAD + hxObese + hxCKD + hxCOPD  
               + hxHTN + ACEi + ARB + BB + statin, data=dataset_all_Rx)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.8ea174d2-a2ff-4ae5-a71f-daa9008aa081"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
library("survival") 
cox_Intx_sex <- function(dataset_all_Rx) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19*sex + age + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_Rx)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.9cec2bb6-60e5-4427-8288-0a193b0470f8"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
library("survival") 
cox_all <- function(dataset_all_update) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN, data=dataset_all_temp)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.9b86f65a-c682-422a-bf44-59dc66819640"),
    dataset_all_F=Input(rid="ri.vector.main.execute.555f06a2-ad3d-4a50-83bc-f41cce076e70")
)
library("survival") 
cox_all_F <- function(dataset_all_F) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_F)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.54c3e98f-5b19-418f-8c69-ce21c6d88570"),
    dataset_all_M=Input(rid="ri.vector.main.execute.afb8595c-da82-430e-ada5-acfa6ecbe19a")
)
library("survival") 
cox_all_M <- function(dataset_all_M) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_M)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.db83510f-713c-4531-8b41-e2c0d2e2fe1c"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
library("survival") 
cox_all_crude <- function(dataset_all_update) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19, data=dataset_all_update)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.fc366337-f8d7-459c-8904-14727f9d58cf"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
library("survival") 
cox_all_dem <- function(dataset_all_update) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup, data=dataset_all_temp)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.d7a232ff-4159-4e6e-9f3d-aa81a35f46f7"),
    dataset_all_nonwhite=Input(rid="ri.vector.main.execute.4607ebab-6a6e-4075-81b8-d290bb1784e5")
)
library("survival") 
cox_all_nonwhite <- function(dataset_all_nonwhite) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_nonwhite)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.500b539b-e470-43d1-88a9-ad0030b6cef0"),
    dataset_all_over65=Input(rid="ri.vector.main.execute.99845b4a-5950-466d-9366-4758069efd5a")
)
library("survival") 
cox_all_over65 <- function(dataset_all_over65) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_over65)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.4703ae1e-6654-486e-9bce-0ec71cf544d0"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
library("survival") 
cox_all_rx <- function(dataset_all_Rx) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN 
        + ACEi + ARB + BB + statin,
        data=dataset_all_Rx)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.80881704-f7fc-4486-9bfb-a2a5cb163c47"),
    dataset_all_under65=Input(rid="ri.vector.main.execute.c6cb4360-24c7-454a-9fd9-e19230e99c12")
)
library("survival") 
cox_all_under65 <- function(dataset_all_under65) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_under65)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.eb56896e-3543-4a73-abfd-e95052820d62"),
    dataset_all_white=Input(rid="ri.vector.main.execute.b0401bbb-c7b7-40d2-ab32-848725e12bc7")
)
library("survival") 
cox_all_white <- function(dataset_all_white) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_white)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.c62fab79-3554-4a0d-ab12-1c46121b0a18"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
library("survival") 
cox_comp <- function(dataset_all_Rx) {
        result <- coxph(Surv(futime_Comp, comp_case) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_Rx)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.687dd142-9f4f-4496-8146-3e0117a10871"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
library("survival") 
cox_death <- function(dataset_all_Rx) {
        result <- coxph(Surv(futime_Death, Death_case) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_all_Rx)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.6e92389e-d5b7-470c-a0fd-33bb8737e661"),
    dataset_No_Rx=Input(rid="ri.vector.main.execute.daf437ee-de2c-4bb3-a390-d4c1e81f97d8")
)
library("survival") 
cox_no_Rx <- function(dataset_No_Rx) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_No_Rx)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.0cde8bc7-21ae-4e34-9d05-f6af53ba74f4"),
    hi_NP_exclude=Input(rid="ri.vector.main.execute.b3976f27-926c-4bb7-8247-36cee65c57f9")
)
library("survival") 
cox_no_hi_NP <- function(hi_NP_exclude) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=hi_NP_exclude)
    print(result)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.afb416a4-9f77-438c-a728-f5bcc0e0b37d"),
    dataset_with_Rx=Input(rid="ri.vector.main.execute.d5572b3d-7264-40ff-b6d1-31f86981f174")
)
library("survival") 
cox_with_Rx <- function(dataset_with_Rx) {
        result <- coxph(Surv(futime_all, HFcase) ~ c19 + age + sex + racegroup + hxDM + hxCAD + hxObese + hxCKD + hxCOPD + hxHTN
        + ACEi + ARB + BB + statin, data=dataset_with_Rx)
    print(result)
    return(NULL)
}

