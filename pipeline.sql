

@transform_pandas(
    Output(rid="ri.vector.main.execute.a2a64bda-1313-4cb7-957d-1903140ec131"),
    ACE_dataset=Input(rid="ri.vector.main.execute.88b9b130-1763-4c8b-b237-cbfa61cc36e6"),
    first_ARB=Input(rid="ri.vector.main.execute.8c17d423-a838-4a58-8155-5ba10eafd3e0")
)
SELECT  first_ARB.ARB_prior,
        ACE_dataset.ACEi_prior,
        ACE_dataset.patient_id
FROM ACE_dataset left join first_ARB
ON ACE_dataset.patient_id = first_ARB.ARB_ID

@transform_pandas(
    Output(rid="ri.vector.main.execute.d0904f4b-a771-4759-97d1-33dc7a611a52"),
    ACE_ARB=Input(rid="ri.vector.main.execute.a2a64bda-1313-4cb7-957d-1903140ec131"),
    first_BB=Input(rid="ri.vector.main.execute.2653889b-f275-47f0-b337-e310573c07cc")
)
SELECT  ACE_ARB.ACEi_prior,
        ACE_ARB.ARB_prior,
        ACE_ARB.patient_id,
        first_BB.BB_prior
FROM ACE_ARB left join first_BB
ON ACE_ARB.patient_id = first_BB.BB_ID

@transform_pandas(
    Output(rid="ri.vector.main.execute.5ab56f4d-721a-4669-aede-56d08d87785a"),
    ACE_ARB_BB=Input(rid="ri.vector.main.execute.d0904f4b-a771-4759-97d1-33dc7a611a52"),
    first_statin=Input(rid="ri.vector.main.execute.e9ec7d30-5cb3-4290-a22c-c3b841853993")
)
SELECT  ACE_ARB_BB.patient_id as Rx_ID,
        ACE_ARB_BB.ACEi_prior,
        ACE_ARB_BB.ARB_prior,
        ACE_ARB_BB.BB_prior,
        first_statin.statin_prior
FROM ACE_ARB_BB left join first_statin
ON ACE_ARB_BB.patient_id = first_statin.statin_ID

@transform_pandas(
    Output(rid="ri.vector.main.execute.88b9b130-1763-4c8b-b237-cbfa61cc36e6"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d"),
    first_ACEi=Input(rid="ri.vector.main.execute.f9744ba9-81eb-434f-85fe-050eaba77f65")
)
SELECT first_ACEi.ACEi_prior,
        dataset_all_update.patient_id
FROM dataset_all_update left join first_ACEi
ON dataset_all_update.patient_id = first_ACEi.ACEi_ID

@transform_pandas(
    Output(rid="ri.vector.main.execute.3032fa50-5f50-4582-aff8-071447903533"),
    ACEi=Input(rid="ri.vector.main.execute.14e2d459-6162-459a-a839-651da0148eb7"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT  ACEi.person_id as ACE_patient,
        ACEi.drug_concept_name as ACE,
        ACEi.drug_era_start_date as ACEi_start,
       dataset_all_update.c19,
       dataset_all_update.HFcase
FROM dataset_all_update left join ACEi
ON dataset_all_update.patient_id = ACEi.person_id
WHERE ACEi.drug_era_start_date <= dataset_all_update.index_dc and ACEi.person_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.14e2d459-6162-459a-a839-651da0148eb7"),
    drug_era=Input(rid="ri.foundry.main.dataset.4f424984-51a6-4b10-9b2b-0410afa1b2f8")
)
SELECT  person_id,
        drug_concept_name,
        drug_era_start_date
FROM drug_era
WHERE drug_concept_name like '%pril%' and drug_concept_name not like '%prilo'

@transform_pandas(
    Output(rid="ri.vector.main.execute.8c599b99-673f-47fc-94d4-4758bfbdd172"),
    ACE_prior=Input(rid="ri.vector.main.execute.3032fa50-5f50-4582-aff8-071447903533")
)
select  c19, 
        count(1) as num_records,          
        count(distinct ACE_patient)       
from ACE_prior
group by c19
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.a04bda5d-02c6-4bde-904e-008ed1fdf088"),
    drug_era=Input(rid="ri.foundry.main.dataset.4f424984-51a6-4b10-9b2b-0410afa1b2f8")
)
SELECT  person_id,
        drug_concept_name,
        drug_era_start_date
FROM drug_era
WHERE drug_concept_name like '%sartan%' 

@transform_pandas(
    Output(rid="ri.vector.main.execute.c42b1253-2349-4c6b-a05f-39b544b9b03e"),
    ARB_prior=Input(rid="ri.vector.main.execute.909625ae-9ff2-4526-8e17-f2eb1812f8e0")
)
select  c19,
        count(1) as num_records ,       
        count(distinct ARB_patient)       
from ARB_prior
group by c19
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.909625ae-9ff2-4526-8e17-f2eb1812f8e0"),
    ARB=Input(rid="ri.vector.main.execute.a04bda5d-02c6-4bde-904e-008ed1fdf088"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT  ARB.person_id as ARB_patient,
        ARB.drug_concept_name as ARBs,
        ARB.drug_era_start_date as ARB_start,
       dataset_all_update.c19,
       dataset_all_update.HFcase
FROM dataset_all_update left join ARB
ON dataset_all_update.patient_id = ARB.person_id
WHERE ARB.drug_era_start_date <= dataset_all_update.index_dc and ARB.person_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.78b5a66c-2791-4a95-94c9-32ceaadf31d3"),
    drug_era=Input(rid="ri.foundry.main.dataset.4f424984-51a6-4b10-9b2b-0410afa1b2f8")
)
SELECT  person_id,
        drug_concept_name,
        drug_era_start_date
FROM drug_era
WHERE drug_concept_name like '%lol%' and drug_concept_name not like '%timolol%' and drug_concept_name not like '%olamide%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.e063812f-c740-4225-8533-8a49284a3ab9"),
    BB_prior=Input(rid="ri.vector.main.execute.30c41ee1-ffca-4731-8e48-13a243f74a73")
)
select  c19,
        count(1) as num_records ,       
        count(distinct BB_patient)       
from BB_prior
group by c19
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.30c41ee1-ffca-4731-8e48-13a243f74a73"),
    BB=Input(rid="ri.vector.main.execute.78b5a66c-2791-4a95-94c9-32ceaadf31d3"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT  BB.person_id as BB_patient,
        BB.drug_concept_name as BetaBlocker,
        BB.drug_era_start_date as BB_start,
       dataset_all_update.c19,
       dataset_all_update.HFcase
FROM dataset_all_update left join BB
ON dataset_all_update.patient_id = BB.person_id
WHERE BB.drug_era_start_date <= dataset_all_update.index_dc and BB.person_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.2920866d-8ab3-455e-804c-a8b48de04553"),
    bnp_data=Input(rid="ri.foundry.main.dataset.ef050a76-d4ea-4fb3-9ae3-f732cdf505f5")
)
SELECT *
FROM bnp_data 
WHERE (bnp_data.bnp_type not like '%prohormone%') and bnp_value is not null 

@transform_pandas(
    Output(rid="ri.vector.main.execute.80d71f27-f827-442b-8b9f-8773aeff74fa"),
    first_BNP=Input(rid="ri.vector.main.execute.e876cb8b-89b0-4896-a299-3608993793ac")
)
select  c19_status, 
        count(1) as num_records,     
        percentile_approx(BNP_level, 0.5)  as median,
        percentile_approx(BNP_level, 0.25) as q1,
        percentile_approx(BNP_level, 0.75) as q3         
from first_BNP
group by c19_status
order by num_records asc 

@transform_pandas(
    Output(rid="ri.vector.main.execute.3e925583-4eff-4b96-8444-a3667614a94d"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85"),
    hi_NP_patient=Input(rid="ri.vector.main.execute.e2bfbe5b-d6c2-41ed-9af0-106975eade88")
)
SELECT *
FROM dataset_all_Rx left join hi_NP_patient
ON dataset_all_Rx.Rx_ID = hi_NP_patient.hi_NP_ID

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0b1d3124-c55f-4968-906d-d150d866eee6"),
    BP_macrovisit=Input(rid="ri.vector.main.execute.7e5db0e4-a60d-4ae6-a864-f5621776e037"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT  BP_macrovisit.bp_patient,
        BP_macrovisit.bp_value,
        BP_macrovisit.bp_date, 
        BP_macrovisit.bp_type,       
        dataset_all_update.c19,
        dataset_all_update.HFcase
FROM dataset_all_update left join BP_macrovisit
ON dataset_all_update.macrovisit_num = BP_macrovisit.macrovisit_id
WHERE BP_macrovisit.bp_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.7e5db0e4-a60d-4ae6-a864-f5621776e037"),
    BPs=Input(rid="ri.vector.main.execute.ac965843-d6d4-4f0d-8689-e0d9960c6800"),
    measurements_to_macrovisits=Input(rid="ri.foundry.main.dataset.7d655791-94cc-4322-b9e9-ce66308126f5")
)
SELECT  BPs.bp_patient,
        BPs.bp_date,
        BPs.bp_value,
        BPs.bp_type,
        measurements_to_macrovisits.macrovisit_id
FROM BPs left join measurements_to_macrovisits 
ON BPs.bp_measurement_id = measurements_to_macrovisits.measurement_id  
WHERE measurements_to_macrovisits.macrovisit_id is not null and bp_value is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.ac965843-d6d4-4f0d-8689-e0d9960c6800"),
    measurement=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT  person_id as bp_patient,
        measurement_id as bp_measurement_id,
        measurement_concept_name as bp_type,
        value_as_number as bp_value,
        measurement_date as bp_date
FROM measurement
WHERE measurement_concept_name like '%blood pressure%' and measurement_concept_name not like '%mean%' and measurement_concept_name not like '%Mean%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.7f09b34e-a47f-497a-80c3-d619a7fc4849"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as CAD_id, 
        condition_concept_name as CAD_descrip,
        condition_start_date as CAD_start      
FROM condition_occurrence
Where condition_concept_name like '%coronary artery%' or condition_concept_name like '%Coronary artery%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.be8f27da-3368-4ed0-ba37-d5fbf716b420"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as CKD_id, 
        condition_concept_name as CKD_descrip,
        condition_start_date as CKD_start      
FROM condition_occurrence
Where condition_concept_name like '%chronic kidney disease%' or condition_concept_name like '%Chronic kidney disease%' 
        or condition_concept_name like '%renal disease%' or condition_concept_name like '%Renal disease%' 
        or condition_concept_name like '%renal insufficiency%' or condition_concept_name like '%Renal insufficiency%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.ed2e0f57-806f-452a-95a6-f0a1d96ed8df"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as COPD_id, 
        condition_concept_name as COPD_descrip,
        condition_start_date as COPD_start      
FROM condition_occurrence
Where condition_concept_name like '%pulmonary disease%' or condition_concept_name like '%lung disease%' or condition_concept_name like '%chronic bronchitis' or condition_concept_name like '%Chronic bronchitis'

@transform_pandas(
    Output(rid="ri.vector.main.execute.1d4eb8bc-f8bf-4c15-8db1-b5ca1dad9388"),
    BP_data=Input(rid="ri.foundry.main.dataset.0b1d3124-c55f-4968-906d-d150d866eee6")
)
SELECT *
FROM BP_data
WHERE bp_type like '%Diastolic%' or bp_type like '%diastolic%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.c5b0ccca-9acc-4de6-b06e-0d984632044f"),
    first_DBP=Input(rid="ri.vector.main.execute.3f5467c0-ff8e-4350-bd9c-3607814b16a7")
)
select  c19_status, 
        count(1) as num_records,
        avg(DBP_value) as MeanDBP,
        stddev_samp(DBP_value)        
from first_DBP
group by c19_status
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.c0ff565c-9674-4cd8-8637-c998e9f8f102"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as HFpatient_id, 
        condition_type_concept_name as HF_data,
        condition_concept_name as HF_description,
        condition_start_date as HF_start      
FROM condition_occurrence
Where (condition_concept_name like '%heart failure%' or condition_concept_name like '%Heart failure%')
        and condition_concept_name not like '%Hypertensive heart disease without%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.b0b5038c-b2a1-49fe-8610-2e378b7d4b1f"),
    first_HR=Input(rid="ri.vector.main.execute.3b4e6308-65c7-4c02-8ef7-d0f36e6d6aae")
)
select  c19status, 
        count(1) as num_records,
        avg(HR_bpm) as MeanBP,
        avg(HFstatus)as HFnum
from first_HR
group by c19status, HFstatus
order by num_records asc

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4d49dee1-3e4d-4543-be07-ccca1a7e156d"),
    HRs_macrovisit=Input(rid="ri.vector.main.execute.a945452d-2c4c-4df7-a73a-43c808bf1f49"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT  HRs_macrovisit.hr_patient,
        HRs_macrovisit.hr_value,
        HRs_macrovisit.hr_date,        
        dataset_all_update.c19,
        dataset_all_update.HFcase
FROM dataset_all_update left join HRs_macrovisit
ON dataset_all_update.macrovisit_num = HRs_macrovisit.macrovisit_id
WHERE HRs_macrovisit.hr_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.245f6422-051c-4eb5-a32c-99855005a6a1"),
    first_HR=Input(rid="ri.vector.main.execute.3b4e6308-65c7-4c02-8ef7-d0f36e6d6aae")
)
select  c19status, 
        count(1) as num_records,
        avg(HR_bpm) as MeanHR,
        stddev_samp(HR_bpm)        
from first_HR
group by c19status
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.67784563-55a3-481d-9f6e-a2c50e80f4d6"),
    measurement=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT  person_id as hr_patient,
        measurement_id as hr_measurement_id,
        measurement_concept_name as hr_type,
        value_as_number as hr_value,
        measurement_date as hr_date
FROM measurement
WHERE measurement_concept_name like '%heart rate%' or measurement_concept_name like '%Heart rate%' or measurement_concept_name like '%puse rate%'or measurement_concept_name like '%Pulse rate%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.a945452d-2c4c-4df7-a73a-43c808bf1f49"),
    HRs=Input(rid="ri.vector.main.execute.67784563-55a3-481d-9f6e-a2c50e80f4d6"),
    measurements_to_macrovisits=Input(rid="ri.foundry.main.dataset.7d655791-94cc-4322-b9e9-ce66308126f5")
)
SELECT  HRs.hr_patient,
        HRs.hr_measurement_id,        
        HRs.hr_value,
        HRs.hr_date,
        measurements_to_macrovisits.macrovisit_id
FROM HRs left join measurements_to_macrovisits 
ON HRs.hr_measurement_id = measurements_to_macrovisits.measurement_id  
WHERE measurements_to_macrovisits.macrovisit_id is not null and hr_value is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.b7fec751-19c4-4458-8ad9-9dbc87fcb4db"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as HTN_id, 
        condition_concept_name as HTN_descrip,
        condition_start_date as HTN_start      
FROM condition_occurrence
Where condition_concept_name like '%hypertensi%' or condition_concept_name like '%Hypertensi%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.34e01075-721a-4d2c-be0e-f3af00fcc34d"),
    dataset_all_F=Input(rid="ri.vector.main.execute.555f06a2-ad3d-4a50-83bc-f41cce076e70")
)
SELECT count(patient_id)
FROM dataset_all_F

@transform_pandas(
    Output(rid="ri.vector.main.execute.c6cc1eb4-1580-4889-b41c-efd75ee5a55a"),
    dataset_all_M=Input(rid="ri.vector.main.execute.afb8595c-da82-430e-ada5-acfa6ecbe19a")
)
SELECT count(patient_id)
FROM dataset_all_M

@transform_pandas(
    Output(rid="ri.vector.main.execute.e315a15d-f165-4db5-b569-76c28d55ae99"),
    dataset_No_Rx=Input(rid="ri.vector.main.execute.daf437ee-de2c-4bb3-a390-d4c1e81f97d8")
)
SELECT count(patient_id)
FROM dataset_No_Rx

@transform_pandas(
    Output(rid="ri.vector.main.execute.9c5aab98-a276-48f0-a44f-ca5c56e65964"),
    dataset_all_nonwhite=Input(rid="ri.vector.main.execute.4607ebab-6a6e-4075-81b8-d290bb1784e5")
)
SELECT count(patient_id)
FROM dataset_all_nonwhite

@transform_pandas(
    Output(rid="ri.vector.main.execute.063ac948-8a80-41a4-aa80-595d56a3b570"),
    dataset_all_over65=Input(rid="ri.vector.main.execute.99845b4a-5950-466d-9366-4758069efd5a")
)
SELECT count(patient_id)
FROM dataset_all_over65

@transform_pandas(
    Output(rid="ri.vector.main.execute.d0234bd2-a6d6-47d0-bcc9-0140fa5581be"),
    dataset_all_under65=Input(rid="ri.vector.main.execute.c6cb4360-24c7-454a-9fd9-e19230e99c12")
)
SELECT count (patient_id)        
FROM dataset_all_under65

@transform_pandas(
    Output(rid="ri.vector.main.execute.5a5b5c17-1862-4943-b780-8b403970044b"),
    dataset_all_white=Input(rid="ri.vector.main.execute.b0401bbb-c7b7-40d2-ab32-848725e12bc7")
)
SELECT count(patient_id)
FROM dataset_all_white

@transform_pandas(
    Output(rid="ri.vector.main.execute.011b0fa1-7470-4083-942d-326e99307424"),
    dataset_with_Rx=Input(rid="ri.vector.main.execute.d5572b3d-7264-40ff-b6d1-31f86981f174")
)
SELECT count(patient_id)
FROM dataset_with_Rx

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d482e4c9-bfda-4b9e-9532-72b27defb2d0"),
    RespRate_macrovisit=Input(rid="ri.vector.main.execute.9300c5d4-c3f1-4f09-96c9-767a00e85e77"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT  RespRate_macrovisit.rr_patient,
        RespRate_macrovisit.rr_value,
        RespRate_macrovisit.rr_date,       
        dataset_all_update.c19,
        dataset_all_update.HFcase
FROM dataset_all_update left join RespRate_macrovisit
ON dataset_all_update.macrovisit_num = RespRate_macrovisit.macrovisit_id
WHERE RespRate_macrovisit.rr_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.9300c5d4-c3f1-4f09-96c9-767a00e85e77"),
    RespRates=Input(rid="ri.vector.main.execute.228c09b0-d2a7-4baa-974d-2e7cd96fc895"),
    measurements_to_macrovisits=Input(rid="ri.foundry.main.dataset.7d655791-94cc-4322-b9e9-ce66308126f5")
)
SELECT  RespRates.rr_patient,
        RespRates.rr_date,
        RespRates.rr_type,
        RespRates.rr_value,
        measurements_to_macrovisits.macrovisit_id
FROM RespRates left join measurements_to_macrovisits 
ON RespRates.rr_measurement_id = measurements_to_macrovisits.measurement_id  
WHERE measurements_to_macrovisits.macrovisit_id is not null 

@transform_pandas(
    Output(rid="ri.vector.main.execute.17d127c4-333a-4748-bc9d-e2de40126586"),
    first_RespRate=Input(rid="ri.vector.main.execute.82e52ef0-11b6-4283-b25b-bb3865993fdf")
)
select  c19_status, 
        count(1) as num_records,
        avg(RespRate_value) as MeanRespRate,
        stddev_samp(RespRate_value),
        min(RespRate_value),
        max(RespRate_value)        
from first_RespRate
group by c19_status
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.228c09b0-d2a7-4baa-974d-2e7cd96fc895"),
    measurement=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT  person_id as rr_patient,
        measurement_id as rr_measurement_id,
        measurement_concept_name as rr_type,
        value_as_number as rr_value,
        measurement_date as rr_date
FROM measurement
WHERE measurement_concept_name like '%Respiratory rate%' or measurement_concept_name like '%respiratory rate%' 

@transform_pandas(
    Output(rid="ri.vector.main.execute.a2bb2762-ca6d-4a01-bcc8-ae9eee9b3b97"),
    BP_data=Input(rid="ri.foundry.main.dataset.0b1d3124-c55f-4968-906d-d150d866eee6")
)
SELECT *
FROM BP_data
WHERE bp_type like '%Systolic%' or bp_type like '%systolic%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.8d8ce5b7-69e6-4cb1-a83a-84c32e4fedb8"),
    first_SBP=Input(rid="ri.vector.main.execute.92f973fc-6da4-4606-b53c-79015a9f67b7")
)
select  c19_status, 
        count(1) as num_records,
        avg(SBP_value) as MeanSBP,
        stddev_samp(SBP_value)        
from first_SBP
group by c19_status
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.1818dfe8-1d8a-43b6-9de7-bcff230b57b8"),
    tn_data=Input(rid="ri.foundry.main.dataset.878498d5-3a01-4815-adab-6e92f85a8db0")
)
SELECT *
FROM tn_data
WHERE tn_type like '%I.cardiac%' or tn_type like '%I measurement%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.090dd45a-4585-4f00-b2c1-cc99532724a9"),
    first_TnI=Input(rid="ri.vector.main.execute.92c9353a-8f43-4456-9011-e27be21e9a64")
)
select  c19_status, 
        count(1) as num_records,     
        percentile_approx(TnI_level, 0.5)  as median,
        percentile_approx(TnI_level, 0.25) as q1,
        percentile_approx(TnI_level, 0.75) as q3         
from first_TnI
group by c19_status
order by num_records asc 

@transform_pandas(
    Output(rid="ri.vector.main.execute.1a9af0be-71e8-404b-816f-960673b62169"),
    tn_data=Input(rid="ri.foundry.main.dataset.878498d5-3a01-4815-adab-6e92f85a8db0")
)
SELECT *
FROM tn_data
WHERE tn_type like '%T.cardiac%' or tn_type like '%T measurement%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.922806cb-e8f5-481f-bfb8-5032435c75b2"),
    first_TnT=Input(rid="ri.vector.main.execute.76ea253c-7104-446f-a476-55165a272bbc")
)
select  c19_status, 
        count(1) as num_records,     
        percentile_approx(TnT_level, 0.5)  as median,
        percentile_approx(TnT_level, 0.25) as q1,
        percentile_approx(TnT_level, 0.75) as q3         
from first_TnT
group by c19_status
order by num_records asc 

@transform_pandas(
    Output(rid="ri.vector.main.execute.122dba48-ef69-4e2b-bac7-b94327a20182"),
    first_Tn_unk=Input(rid="ri.vector.main.execute.5feccb73-b9bf-4532-9111-5418d2211f43")
)
SELECT count(distinct Tn_unk_ID),
        avg(Tn_unk_level)
FROM first_Tn_unk

@transform_pandas(
    Output(rid="ri.vector.main.execute.661f9c19-4a69-4b15-9bfb-f14d32befeda"),
    tn_data=Input(rid="ri.foundry.main.dataset.878498d5-3a01-4815-adab-6e92f85a8db0")
)
SELECT *
FROM tn_data
WHERE tn_type not like '%I.cardiac%' and tn_type not like '%I measurement%' and 
      tn_type not like '%T.cardiac%' and tn_type not like '%T measurement%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.6179479c-a2b8-4a31-af91-f564f1cee1cd"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as anemia_id, 
        condition_concept_name as anemia_descrip,
        condition_start_date as anemia_start      
FROM condition_occurrence
Where condition_concept_name like '%Anemia%' or condition_concept_name like '%anemia%' 

@transform_pandas(
    Output(rid="ri.vector.main.execute.757956c2-6750-453b-86e3-d185f1612ba3"),
    first_BNP=Input(rid="ri.vector.main.execute.e876cb8b-89b0-4896-a299-3608993793ac")
)
select  c19_status, 
        count(1) as num_records,
        avg(BNP_level) as MeanBNP,
        avg(HF_status) as HFnum     
from first_BNP
group by c19_status, HF_status
order by num_records asc

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ef050a76-d4ea-4fb3-9ae3-f732cdf505f5"),
    bnp_macrovisit=Input(rid="ri.vector.main.execute.1a61b4b4-30db-479a-9131-78a9b35951f5"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT  bnp_macrovisit.bnp_patient,
        bnp_macrovisit.bnp_value,
        bnp_macrovisit.bnp_date,
        bnp_macrovisit.bnp_type,
        dataset_all_update.c19,
        dataset_all_update.HFcase
FROM dataset_all_update left join bnp_macrovisit
ON dataset_all_update.macrovisit_num = bnp_macrovisit.macrovisit_id
WHERE bnp_macrovisit.bnp_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.7f9cf6cd-db71-485b-b2c7-61186ad71760"),
    measurement=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT  person_id as bnp_patient,
        measurement_id as bnp_measurement_id,
        measurement_concept_name as bnp_type,
        value_as_number as bnp_value,
        measurement_date as bnp_date
FROM measurement
WHERE (measurement_concept_name like '%natriuretic%' or measurement_concept_name like '%Natriuretic%') and value_as_number is not null and measurement_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.1a61b4b4-30db-479a-9131-78a9b35951f5"),
    bnp_labs=Input(rid="ri.vector.main.execute.7f9cf6cd-db71-485b-b2c7-61186ad71760"),
    measurements_to_macrovisits=Input(rid="ri.foundry.main.dataset.7d655791-94cc-4322-b9e9-ce66308126f5")
)
SELECT  bnp_labs.bnp_patient,
        bnp_labs.bnp_date,
        bnp_labs.bnp_type,
        bnp_labs.bnp_value,
        measurements_to_macrovisits.macrovisit_id
FROM bnp_labs left join measurements_to_macrovisits 
ON bnp_labs.bnp_measurement_id = measurements_to_macrovisits.measurement_id  
WHERE measurements_to_macrovisits.macrovisit_id is not null 

@transform_pandas(
    Output(rid="ri.vector.main.execute.967bfaa1-bf74-4f76-a1bb-6d8b2942f902"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    first_CAD=Input(rid="ri.foundry.main.dataset.9982d9a0-564f-49d1-a370-14b8b1f09d19")
)
SELECT  c19_IRtime_update.patient_id,
        first_CAD.CAD_patient,
        case when ((first_CAD.first_CAD_date is null) or (c19_IRtime_update.index_dc < first_CAD.first_CAD_date)) then 0 else 1 end as CAD_hx               
FROM c19_IRtime_update left join first_CAD
on c19_IRtime_update.patient_id = first_CAD.CAD_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.488941a1-60c7-47ad-9dba-8fff870d3986"),
    c19_CAD=Input(rid="ri.vector.main.execute.967bfaa1-bf74-4f76-a1bb-6d8b2942f902")
)
SELECT count(patient_id)
FROM c19_CAD
where CAD_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.2f49dac1-2786-4fd1-9222-8a1858d0c7e0"),
    c19_CAD=Input(rid="ri.vector.main.execute.967bfaa1-bf74-4f76-a1bb-6d8b2942f902"),
    c19_obese=Input(rid="ri.vector.main.execute.6f37a1eb-08ca-4ea3-bf1c-70e8b2b55f4a")
)
SELECT  c19_obese.patient_id,
        c19_obese.obese_hx,
        c19_CAD.CAD_hx
FROM c19_CAD inner join c19_obese
on c19_CAD.patient_id = c19_obese.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.0d0f212d-6b37-464f-8cc9-b1feade5d8c4"),
    c19_CADobese=Input(rid="ri.vector.main.execute.2f49dac1-2786-4fd1-9222-8a1858d0c7e0"),
    c19_CKD=Input(rid="ri.vector.main.execute.ad462052-f38e-46a2-b021-459e21fe6b93")
)
SELECT  c19_CADobese.patient_id,
        c19_CADobese.CAD_hx,
        c19_CADobese.obese_hx,
        c19_CKD.CKD_hx
FROM c19_CADobese inner join c19_CKD
on c19_CADobese.patient_id = c19_CKD.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.ad462052-f38e-46a2-b021-459e21fe6b93"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    first_CKD=Input(rid="ri.foundry.main.dataset.7dc5eb1b-8580-42e5-9b3a-81d64ced7431")
)
SELECT  c19_IRtime_update.patient_id,
        first_CKD.CKD_patient,
         case when ((first_CKD.first_CKD_date is null) or (c19_IRtime_update.index_dc < first_CKD.first_CKD_date)) then 0 else 1 end as CKD_hx       
FROM c19_IRtime_update left join first_CKD
on c19_IRtime_update.patient_id = first_CKD.CKD_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.0f3be788-16ae-45a0-858e-6099893fab3c"),
    c19_CKD=Input(rid="ri.vector.main.execute.ad462052-f38e-46a2-b021-459e21fe6b93")
)
SELECT count(patient_id)
FROM c19_CKD
where CKD_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.ec4d4201-cb9c-47ac-811e-8d78ba4a20cc"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    first_COPD=Input(rid="ri.foundry.main.dataset.6feae61c-75ec-4e9b-81a3-4ddfb0662b4b")
)
SELECT  c19_IRtime_update.patient_id,
        first_COPD.COPD_patient,
        case when ((first_COPD.first_COPD_date is null) or (c19_IRtime_update.index_dc < first_COPD.first_COPD_date)) then 0 else 1 end as COPD_hx        
FROM c19_IRtime_update left join first_COPD
on c19_IRtime_update.patient_id = first_COPD.COPD_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.be881480-5441-42fc-9905-d2c98af19312"),
    c19_COPD=Input(rid="ri.vector.main.execute.ec4d4201-cb9c-47ac-811e-8d78ba4a20cc")
)
SELECT count(patient_id)
FROM c19_COPD
where COPD_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.c40deaed-d9a2-4bf0-8f65-440eb077acfb"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    first_DM=Input(rid="ri.foundry.main.dataset.bbc5a5ab-0870-4e6d-baf5-d4d1d962ef26")
)
SELECT  c19_IRtime_update.patient_id,
        c19_IRtime_update.HF,              
        first_DM.DM_patient,
        case when ((first_DM.first_DM_date is null) or (c19_IRtime_update.index_dc < first_DM.first_DM_date)) then 0 else 1 end as DM_hx   
FROM c19_IRtime_update left join first_DM
on c19_IRtime_update.patient_id = first_DM.DM_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.a58057af-9b14-43d6-94e4-57e2ce91a472"),
    c19_DM=Input(rid="ri.vector.main.execute.c40deaed-d9a2-4bf0-8f65-440eb077acfb")
)
SELECT  count(patient_id)                
FROM c19_DM
Where DM_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.54dfe0ea-9f46-4e65-8c8a-1f3a5bc477b7"),
    c19_fu_parent_update=Input(rid="ri.foundry.main.dataset.038991ec-046c-46ea-b7a5-0040a72a31f7")
)
SELECT  patient_id as DeathNoHF_ID,
        datediff (death_date, index_dc) as fuDeath_NoHF        
FROM c19_fu_parent_update
WHERE (HF_id is null) and (death_date is not null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.7aafdae2-2ae7-468a-a300-9c033ac68943"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT  HF,
        futime_HF,
        case when (futime_HF <=30) then 1 else 0 end as HF30    
FROM c19_IRtime_update
WHERE HF is not null 

@transform_pandas(
    Output(rid="ri.vector.main.execute.912df5d2-0277-45d5-b605-18691a63aed5"),
    c19_HF30=Input(rid="ri.vector.main.execute.7aafdae2-2ae7-468a-a300-9c033ac68943")
)
SELECT  sum(HF30),
        sum(HF),
        avg(futime_HF),
        stddev_samp(futime_HF),
        percentile_approx(futime_HF, 0.5) as median,
        percentile_approx(futime_HF, 0.25) as q1,
        percentile_approx(futime_HF, 0.75) as q3
FROM c19_HF30

@transform_pandas(
    Output(rid="ri.vector.main.execute.a130e863-bdbf-4993-9a48-5f9350827e95"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT  first_HF_descrip,
        count(*)
FROM c19_IRtime_update
where HF is not null
Group by first_HF_descrip

@transform_pandas(
    Output(rid="ri.vector.main.execute.066c4682-82ca-4d0b-87f1-04aede9996a4"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    first_HTN=Input(rid="ri.foundry.main.dataset.13b4990e-0a61-45db-8369-3baec3b00bd4")
)
SELECT  c19_IRtime_update.patient_id,
        first_HTN.HTN_patient,
         case when ((first_HTN.first_HTN_date is null) or (c19_IRtime_update.index_dc < first_HTN.first_HTN_date)) then 0 else 1 end as HTN_hx             
FROM c19_IRtime_update left join first_HTN
on c19_IRtime_update.patient_id = first_HTN.HTN_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.c8a95e41-e5a5-40a4-89fb-97dfb8eaafd9"),
    c19_HTN=Input(rid="ri.vector.main.execute.066c4682-82ca-4d0b-87f1-04aede9996a4")
)
SELECT count(patient_id)
FROM c19_HTN
where HTN_patient is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    c19_demfu=Input(rid="ri.vector.main.execute.c9e9d2e6-fdbb-457c-b310-4fafad7512b4")
)
SELECT *,
        (futime_DeathNoHF + futime_NoEvent) as futime_NoHF,
        (futime_DeathNoHF + futime_NoEvent + futime_HF) as futime_all,
        (HF_id / HF_id) as HF
FROM c19_demfu

@transform_pandas(
    Output(rid="ri.vector.main.execute.e11911d1-6717-4240-85f8-4f467441e15b"),
    index_c19=Input(rid="ri.vector.main.execute.6d2cbe70-bed8-4d26-8a3c-30acc710e082")
)
SELECT  count(patient_id1),
        count(distinct patient_id1) 
FROM index_c19

@transform_pandas(
    Output(rid="ri.vector.main.execute.669eb761-d227-4fae-a11a-155279982433"),
    c19_fu_parent_update=Input(rid="ri.foundry.main.dataset.038991ec-046c-46ea-b7a5-0040a72a31f7")
)
SELECT  patient_id as noevent_id,
        datediff (surv_end_date, index_dc) as fuNoEvent 
FROM c19_fu_parent_update
WHERE (death_date is null) and (HF_id is null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.36de13ef-fb21-4b5b-8b19-4a929fb7f317"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT ethnicity,
        count(*)
FROM c19_IRtime_update
WHERE race like '%null%' or race like '%Unknown%' or race like '%No information%' or race like '%Refuse%' or race like '%No matching%'
group by ethnicity

@transform_pandas(
    Output(rid="ri.vector.main.execute.ddc141b0-bddd-4aa1-8e45-d9e8aa64924f"),
    c19_CADobeseCKD=Input(rid="ri.vector.main.execute.0d0f212d-6b37-464f-8cc9-b1feade5d8c4"),
    c19_DM=Input(rid="ri.vector.main.execute.c40deaed-d9a2-4bf0-8f65-440eb077acfb")
)
SELECT  c19_CADobeseCKD.patient_id,
        c19_CADobeseCKD.CAD_hx,
        c19_CADobeseCKD.obese_hx,
        c19_CADobeseCKD.CKD_hx,
        c19_DM.DM_hx
FROM c19_CADobeseCKD inner join c19_DM
on c19_CADobeseCKD.patient_id = c19_DM.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.4f96cd6d-eed5-45f5-a2c9-65858bb166be"),
    c19_COPD=Input(rid="ri.vector.main.execute.ec4d4201-cb9c-47ac-811e-8d78ba4a20cc"),
    c19_comorbid=Input(rid="ri.vector.main.execute.ddc141b0-bddd-4aa1-8e45-d9e8aa64924f")
)
SELECT  c19_comorbid.patient_id,
        c19_comorbid.CAD_hx,
        c19_comorbid.obese_hx,
        c19_comorbid.CKD_hx,
        c19_comorbid.DM_hx,
        c19_COPD.COPD_hx
FROM c19_comorbid inner join c19_COPD
on c19_comorbid.patient_id = c19_COPD.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.bca3bd2d-7652-4f85-af23-bc9cf4e8bc51"),
    c19_HTN=Input(rid="ri.vector.main.execute.066c4682-82ca-4d0b-87f1-04aede9996a4"),
    c19_comorbid_COPD=Input(rid="ri.vector.main.execute.4f96cd6d-eed5-45f5-a2c9-65858bb166be")
)
SELECT  c19_comorbid_COPD.patient_id,
        c19_comorbid_COPD.CAD_hx,
        c19_comorbid_COPD.obese_hx,
        c19_comorbid_COPD.CKD_hx,
        c19_comorbid_COPD.DM_hx,
        c19_comorbid_COPD.COPD_hx,
        c19_HTN.HTN_hx
FROM c19_comorbid_COPD inner join c19_HTN
on c19_comorbid_COPD.patient_id = c19_HTN.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.539a6a3e-06c7-4340-86a9-70eaa56dab1d"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT  count(patient_id),
        count(distinct patient_id),
        count (HF),
        count(DeathNoHF_ID),
        count(noevent_id),
        count(death_id),
        percentile_approx(fuHF, 0.5) as HFmedian,
        percentile_approx(fuHF, 0.25) as HFq1,
        percentile_approx(fuHF, 0.75) as HFq3,
        percentile_approx(fuDeath, 0.5) as Deathmedian,
        percentile_approx(fuDeath, 0.25) as Deathq1,
        percentile_approx(fuDeath, 0.75) as Deathq3,
        percentile_approx(futime_all, 0.5) as Allmedian,
        percentile_approx(futime_all, 0.25) as Allq1,
        percentile_approx(futime_all, 0.75) as Allq3,
        sum(futime_HF),        
        sum(futime_DeathNoHF),        
        sum(futime_NoEvent),       
        sum(futime_NoHF),        
        sum(futime_Death),        
        sum(futime_all),        
        avg(fuHF),
        stddev_samp(fuHF),
        avg(fuDeath),
        stddev_samp(fuDeath),
        avg(fuDeath_NoHF),
        stddev_samp(fuDeath_NoHF),
        avg(fuNoEvent),
        stddev_samp(fuNoEvent),
        avg(futime_all),
        stddev_samp(futime_all)
FROM c19_IRtime_update

@transform_pandas(
    Output(rid="ri.vector.main.execute.2920fa8a-0815-4d70-8c62-1603cf92cc75"),
    c19_noHxHF=Input(rid="ri.vector.main.execute.e6b39eb8-7215-49e0-8be9-24df255c37d6"),
    deathfile=Input(rid="ri.vector.main.execute.5cb733b6-7e55-488d-9f8f-8d337fc1a53a")
)
SELECT *
FROM c19_noHxHF left join deathfile
on c19_noHxHF.patient_id = deathfile.death_id
where index_dc > '2020-03-01'

@transform_pandas(
    Output(rid="ri.vector.main.execute.e24d098e-0e8f-4f5b-aed0-1169ac05d8ae"),
    c19_postDC=Input(rid="ri.vector.main.execute.267ae69d-0597-4a36-b9d9-6b9f7dc04192")
)
SELECT *
FROM c19_postDC
where death_date is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.c9e9d2e6-fdbb-457c-b310-4fafad7512b4"),
    c19_demfu_noAge=Input(rid="ri.vector.main.execute.21f0e7c3-f93b-4ebb-998f-666e84740e6e")
)
SELECT *,
        (2022 - birthyear) as age,
        coalesce(fuHF, 0) as futime_HF,
        coalesce(fuDeath_NoHF, 0) as futime_DeathNoHF,
        coalesce(fuNoEvent, 0) as futime_NoEvent,
        coalesce(fuDeath, 0) as futime_Death,
        1 as c19
from c19_demfu_noAge

@transform_pandas(
    Output(rid="ri.vector.main.execute.21f0e7c3-f93b-4ebb-998f-666e84740e6e"),
    c19_fu=Input(rid="ri.vector.main.execute.dd101097-0829-4095-8418-2df3abe6b674"),
    dems=Input(rid="ri.vector.main.execute.38b9ca54-d851-41f6-b4f9-09a683075ca2")
)
SELECT *
FROM c19_fu left join dems
on c19_fu.patient_id = dems.dem_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.90fc9a4f-f26a-43f4-a0c2-89bea5af5430"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT ethnicity,
        count(*)
FROM c19_IRtime
group by ethnicity

@transform_pandas(
    Output(rid="ri.vector.main.execute.dd101097-0829-4095-8418-2df3abe6b674"),
    c19_DeathNoHF_time=Input(rid="ri.vector.main.execute.54dfe0ea-9f46-4e65-8c8a-1f3a5bc477b7"),
    c19_parent_and_NoEvent=Input(rid="ri.vector.main.execute.b9598cd5-f76c-4759-a872-61553c798d6c")
)
SELECT *
FROM c19_parent_and_NoEvent left join c19_DeathNoHF_time
on c19_parent_and_NoEvent.patient_id = c19_DeathNoHF_time.DeathNoHF_ID

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.038991ec-046c-46ea-b7a5-0040a72a31f7"),
    c19_postDC=Input(rid="ri.vector.main.execute.267ae69d-0597-4a36-b9d9-6b9f7dc04192")
)
SELECT  *,
        datediff (first_HF_date, index_dc) as fuHF,
        datediff (death_date, index_dc) as fuDeath,        
        coalesce(death_date, '2022-03-23') as surv_end_date     
FROM c19_postDC

@transform_pandas(
    Output(rid="ri.vector.main.execute.dec41ae7-6778-4a0c-9da9-5db39019af87"),
    index_c19_first=Input(rid="ri.vector.main.execute.73121a1f-9536-495f-ad50-1c51ad0391a0")
)
SELECT count(patient_id)
FROM index_c19_first

@transform_pandas(
    Output(rid="ri.vector.main.execute.abea5342-1425-44ad-bfbe-ed8e2292c332"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT  avg(age),
        stddev_samp(age),
        count(1) as num_age
FROM c19_IRtime_update

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bf95b4bd-8854-432f-9b29-e9827a68170f"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    c19_comorbid_all=Input(rid="ri.vector.main.execute.bca3bd2d-7652-4f85-af23-bc9cf4e8bc51")
)
SELECT  c19_IRtime_update.index_dc,
        c19_IRtime_update.sex,
        c19_IRtime_update.race,
        c19_IRtime_update.ethnicity,
        c19_IRtime_update.age,
        c19_IRtime_update.futime_all,
        c19_IRtime_update.futime_Death,
        (c19_IRtime_update.DeathNoHF_ID / c19_IRtime_update.DeathNoHF_ID) as DeathNoHF,
        (c19_IRtime_update.death_id / c19_IRtime_update.death_id) as Death,
        (c19_IRtime_update.futime_HF + c19_IRtime_update.futime_DeathNoHF + c19_IRtime_update.futime_NoEvent) as futime_Comp,
        case when (c19_IRtime_update.DeathNoHF_ID is null and c19_IRtime_update.HF is null) then 0 else 1 end as comp_case,
        c19_IRtime_update.macrovisit_num,
        coalesce (c19_IRtime_update.HF, 0) as HFcase,        
        c19_IRtime_update.c19,
        c19_comorbid_all.patient_id,
        c19_comorbid_all.CAD_hx,
        c19_comorbid_all.obese_hx,
        c19_comorbid_all.CKD_hx,
        c19_comorbid_all.DM_hx,
        c19_comorbid_all.COPD_hx,
        c19_comorbid_all.HTN_hx
FROM c19_IRtime_update inner join c19_comorbid_all
on c19_IRtime_update.patient_id = c19_comorbid_all.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.5f86e980-9b0a-4f8e-8ee0-d7bf12f79b5c"),
    c19_multivar=Input(rid="ri.foundry.main.dataset.bf95b4bd-8854-432f-9b29-e9827a68170f")
)
SELECT count(patient_id) 
FROM c19_multivar
where age is null

@transform_pandas(
    Output(rid="ri.vector.main.execute.e6b39eb8-7215-49e0-8be9-24df255c37d6"),
    first_HF=Input(rid="ri.vector.main.execute.c39feb8b-b0af-441f-b696-2e75f71302ec"),
    index_c19_first=Input(rid="ri.vector.main.execute.73121a1f-9536-495f-ad50-1c51ad0391a0")
)
SELECT *
FROM index_c19_first left join first_HF
on index_c19_first.patient_id = first_HF.HF_id
where (index_c19_first.index_dc < first_HF.first_HF_date) or (first_HF.first_HF_date is null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.7e806426-e8f8-4c1d-a721-d13cf90f6b2e"),
    c19_noHxHF=Input(rid="ri.vector.main.execute.e6b39eb8-7215-49e0-8be9-24df255c37d6")
)
SELECT  count(patient_id),
        count(distinct patient_id)
FROM c19_noHxHF

@transform_pandas(
    Output(rid="ri.vector.main.execute.6f37a1eb-08ca-4ea3-bf1c-70e8b2b55f4a"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d"),
    first_obese=Input(rid="ri.foundry.main.dataset.1d76a5fe-d0de-4007-9360-069ed05808e1")
)
SELECT  c19_IRtime_update.patient_id,
        first_obese.obese_patient,
        case when ((first_obese.first_obese_date is null) or (c19_IRtime_update.index_dc < first_obese.first_obese_date)) then 0 else 1 end as obese_hx        
FROM c19_IRtime_update left join first_obese
on c19_IRtime_update.patient_id = first_obese.obese_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.b9557bdb-71de-4717-add3-87e8953f4a31"),
    c19_obese=Input(rid="ri.vector.main.execute.6f37a1eb-08ca-4ea3-bf1c-70e8b2b55f4a")
)
SELECT count(patient_id)
FROM c19_obese
where obese_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.b9598cd5-f76c-4759-a872-61553c798d6c"),
    c19_NOevent_time=Input(rid="ri.vector.main.execute.669eb761-d227-4fae-a11a-155279982433"),
    c19_fu_parent_update=Input(rid="ri.foundry.main.dataset.038991ec-046c-46ea-b7a5-0040a72a31f7")
)
SELECT *
FROM c19_fu_parent_update left join c19_NOevent_time
on c19_fu_parent_update.patient_id = c19_NOevent_time.noevent_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.267ae69d-0597-4a36-b9d9-6b9f7dc04192"),
    c19_death_outcome=Input(rid="ri.vector.main.execute.2920fa8a-0815-4d70-8c62-1603cf92cc75")
)
SELECT *
FROM c19_death_outcome
where (index_dc < death_date) or (death_date is null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.e09b123b-1382-41f6-afd7-c7524698646a"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT race,
        count(*)
FROM c19_IRtime_update
group by race

@transform_pandas(
    Output(rid="ri.vector.main.execute.7a3c55f2-0c48-40e8-b789-b3beb881a8a0"),
    c19_IRtime_update=Input(rid="ri.foundry.main.dataset.966c2e76-c46f-41e0-8b8d-b9ab773a460d")
)
SELECT  sex,
        count(*)
FROM c19_IRtime_update
group by sex

@transform_pandas(
    Output(rid="ri.vector.main.execute.f86bd138-46c1-4867-a56a-6679bc3c4149"),
    ACE_ARB_BB_statin=Input(rid="ri.vector.main.execute.5ab56f4d-721a-4669-aede-56d08d87785a")
)
SELECT  ACE_ARB_BB_statin.Rx_ID,
        coalesce(ACEi_prior, 0) as ACEi,
        coalesce(ARB_prior, 0) as ARB,
        coalesce(BB_prior, 0) as BB,
        coalesce(statin_prior, 0) as statin
FROM ACE_ARB_BB_statin

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.affd8c40-b8a7-4a79-b931-da39819027b0"),
    c19_multivar=Input(rid="ri.foundry.main.dataset.bf95b4bd-8854-432f-9b29-e9827a68170f"),
    noC19_multivar=Input(rid="ri.foundry.main.dataset.1643de5a-9154-48e2-97c4-59043804a4e7")
)
SELECT *
FROM noC19_multivar 
where sex in ("FEMALE", "MALE") and age is not null
union
select *
from c19_multivar
where sex in ("FEMALE", "MALE") and age is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f6ecee75-869a-4a54-8f56-b776eec2c0f7"),
    data_all=Input(rid="ri.foundry.main.dataset.affd8c40-b8a7-4a79-b931-da39819027b0"),
    white=Input(rid="ri.vector.main.execute.14a5b1b0-0a6c-4af6-8014-23591498c251")
)
SELECT *
FROM data_all left join white
on data_all.patient_id = white.racegrp_id
where racegrp_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.daf437ee-de2c-4bb3-a390-d4c1e81f97d8"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE ACEi <> 1 and ARB <> 1 and BB <> 1 and statin <> 1

@transform_pandas(
    Output(rid="ri.vector.main.execute.555f06a2-ad3d-4a50-83bc-f41cce076e70"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE sex = "FEMALE"

@transform_pandas(
    Output(rid="ri.vector.main.execute.afb8595c-da82-430e-ada5-acfa6ecbe19a"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE sex = "MALE"

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85"),
    data_Rx=Input(rid="ri.vector.main.execute.f86bd138-46c1-4867-a56a-6679bc3c4149"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d")
)
SELECT *
FROM data_Rx left join dataset_all_update
ON data_Rx.Rx_ID = dataset_all_update.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.4607ebab-6a6e-4075-81b8-d290bb1784e5"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE white = 0

@transform_pandas(
    Output(rid="ri.vector.main.execute.99845b4a-5950-466d-9366-4758069efd5a"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE age >=65

@transform_pandas(
    Output(rid="ri.vector.main.execute.c6cb4360-24c7-454a-9fd9-e19230e99c12"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE age <65

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d"),
    data_all_race=Input(rid="ri.foundry.main.dataset.f6ecee75-869a-4a54-8f56-b776eec2c0f7")
)
SELECT  *,
        coalesce(DeathNoHF, 0) as DeathNoHF_case,
        coalesce(Death, 0) as Death_case
FROM data_all_race

@transform_pandas(
    Output(rid="ri.vector.main.execute.b0401bbb-c7b7-40d2-ab32-848725e12bc7"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE white = 1

@transform_pandas(
    Output(rid="ri.vector.main.execute.b3924844-f15b-438c-b503-bdcef00b6f3b"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *,
        case when age >=65 then 1 else 0 end as agecat,
        case when (ACEi <> 1 and ARB <> 1 and BB <> 1 and statin <> 1) then 0 else 1 end as CVD_Rx
FROM dataset_all_Rx

@transform_pandas(
    Output(rid="ri.vector.main.execute.815049d4-c55d-4fbd-aa0f-e393f2a0916c"),
    data_all=Input(rid="ri.foundry.main.dataset.affd8c40-b8a7-4a79-b931-da39819027b0")
)
SELECT  patient_id as white_id,
        1 as White
FROM data_all
where race = "White"

@transform_pandas(
    Output(rid="ri.vector.main.execute.d5572b3d-7264-40ff-b6d1-31f86981f174"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *
FROM dataset_all_Rx
WHERE (ACEi = 1 or ARB = 1 or BB = 1 or statin = 1)

@transform_pandas(
    Output(rid="ri.vector.main.execute.5cb733b6-7e55-488d-9f8f-8d337fc1a53a"),
    death=Input(rid="ri.foundry.main.dataset.9c6c12b0-8e09-4691-91e4-e5ff3f837e69")
)
SELECT  min(death_date) as death_date,
        first_value(person_id) as death_id
FROM death
WHERE (death_date > '2020-01-01' and death_date < '2022-04-01') or death_date is null
group by person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.38b9ca54-d851-41f6-b4f9-09a683075ca2"),
    person=Input(rid="ri.foundry.main.dataset.af5e5e91-6eeb-4b14-86df-18d84a5aa010")
)
SELECT  min(year_of_birth) as birthyear,
        first_value(person_id) as dem_id,
        first_value(gender_concept_name) as sex,
        first_value(race_concept_name) as race,
        first_value(ethnicity_concept_name) as ethnicity
FROM person
group by person_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.cb27dfcc-e61a-4080-95ad-6ed682cf07e2"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as DM_id, 
        condition_concept_name as DM_descrip,
        condition_start_date as DM_start      
FROM condition_occurrence
Where (condition_concept_name like '%diabetes%' or condition_concept_name like '%Diabetes%')
        and condition_concept_name not like '%Prediabetes%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.f9744ba9-81eb-434f-85fe-050eaba77f65"),
    ACE_prior=Input(rid="ri.vector.main.execute.3032fa50-5f50-4582-aff8-071447903533")
)
SELECT  min(ACEi_start) as first_ACEi_date,
        first_value(ACE_patient) as ACEi_ID,
        (ACE_patient / ACE_patient) as ACEi_prior                
FROM ACE_prior
group by ACE_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.8c17d423-a838-4a58-8155-5ba10eafd3e0"),
    ARB_prior=Input(rid="ri.vector.main.execute.909625ae-9ff2-4526-8e17-f2eb1812f8e0")
)
SELECT  min(ARB_start) as first_ARB_date,
        first_value(ARB_patient) as ARB_ID,
        (ARB_patient / ARB_patient) as ARB_prior                
FROM ARB_prior
group by ARB_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.2653889b-f275-47f0-b337-e310573c07cc"),
    BB_prior=Input(rid="ri.vector.main.execute.30c41ee1-ffca-4731-8e48-13a243f74a73")
)
SELECT  min(BB_start) as first_BB_date,
        first_value(BB_patient) as BB_ID,
        (BB_patient / BB_patient) as BB_prior                
FROM BB_prior
group by BB_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.e876cb8b-89b0-4896-a299-3608993793ac"),
    BNP_labs=Input(rid="ri.vector.main.execute.2920866d-8ab3-455e-804c-a8b48de04553")
)
SELECT  min(bnp_date) as first_BNP,
        first_value(bnp_patient) as BNP_ID,   
        first_value(bnp_value) as BNP_level,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status
FROM BNP_labs
GROUP BY bnp_patient

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9982d9a0-564f-49d1-a370-14b8b1f09d19"),
    CAD=Input(rid="ri.vector.main.execute.7f09b34e-a47f-497a-80c3-d619a7fc4849")
)
SELECT  min(CAD_start) as first_CAD_date,
        first_value(CAD_id) as CAD_patient,
        first_value(CAD_descrip) as first_CAD_descrip
FROM CAD
group by CAD_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7dc5eb1b-8580-42e5-9b3a-81d64ced7431"),
    CKD=Input(rid="ri.vector.main.execute.be8f27da-3368-4ed0-ba37-d5fbf716b420")
)
SELECT  min(CKD_start) as first_CKD_date,
        first_value(CKD_id) as CKD_patient,
        first_value(CKD_descrip) as first_CKD_descrip
FROM CKD
group by CKD_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6feae61c-75ec-4e9b-81a3-4ddfb0662b4b"),
    COPD=Input(rid="ri.vector.main.execute.ed2e0f57-806f-452a-95a6-f0a1d96ed8df")
)
SELECT  min(COPD_start) as first_COPD_date,
        first_value(COPD_id) as COPD_patient,
        first_value(COPD_descrip) as first_COPD_descrip
FROM COPD
group by COPD_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.3f5467c0-ff8e-4350-bd9c-3607814b16a7"),
    DBP_data=Input(rid="ri.vector.main.execute.1d4eb8bc-f8bf-4c15-8db1-b5ca1dad9388")
)
SELECT min(bp_date) as first_DBP,
        first_value (bp_patient) as DBP_ID,
        first_value(bp_value) as DBP_value,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status 
FROM DBP_data
GROUP BY bp_patient

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bbc5a5ab-0870-4e6d-baf5-d4d1d962ef26"),
    diabetes=Input(rid="ri.vector.main.execute.cb27dfcc-e61a-4080-95ad-6ed682cf07e2")
)
SELECT  min(DM_start) as first_DM_date,
        first_value(DM_id) as DM_patient,
        first_value(DM_descrip) as first_DM_descrip
FROM diabetes
group by DM_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.c39feb8b-b0af-441f-b696-2e75f71302ec"),
    HF=Input(rid="ri.vector.main.execute.c0ff565c-9674-4cd8-8637-c998e9f8f102")
)
SELECT  min(HF_start) as first_HF_date,
        first_value(HFpatient_id) as HF_id,
        first_value(HF_data) as first_HF_data,
        first_value(HF_description) as first_HF_descrip
FROM HF
group by HFpatient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.3b4e6308-65c7-4c02-8ef7-d0f36e6d6aae"),
    HR_data=Input(rid="ri.foundry.main.dataset.4d49dee1-3e4d-4543-be07-ccca1a7e156d")
)
SELECT  min(hr_date) as first_HR,
        first_value(hr_patient) as HR_ID,   
        first_value(hr_value) as HR_bpm,
        first_value(c19) as c19status,
        first_value(HFcase) as HFstatus  
FROM HR_data
GROUP BY hr_patient

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.13b4990e-0a61-45db-8369-3baec3b00bd4"),
    HTN=Input(rid="ri.vector.main.execute.b7fec751-19c4-4458-8ad9-9dbc87fcb4db")
)
SELECT  min(HTN_start) as first_HTN_date,
        first_value(HTN_id) as HTN_patient,
        first_value(HTN_descrip) as first_HTN_descrip
FROM HTN
group by HTN_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.82e52ef0-11b6-4283-b25b-bb3865993fdf"),
    RespRate_data=Input(rid="ri.foundry.main.dataset.d482e4c9-bfda-4b9e-9532-72b27defb2d0")
)
SELECT  min(rr_date) as first_RespRate,
        first_value(rr_patient) as RespRate_ID,   
        first_value(rr_value) as RespRate_value,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status
FROM RespRate_data
WHERE rr_value>0 and rr_value<100
GROUP BY rr_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.92f973fc-6da4-4606-b53c-79015a9f67b7"),
    SBP_data=Input(rid="ri.vector.main.execute.a2bb2762-ca6d-4a01-bcc8-ae9eee9b3b97")
)
SELECT min(bp_date) as first_SBP,
        first_value (bp_patient) as SBP_ID,
        first_value(bp_value) as SBP_value,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status 
FROM SBP_data
GROUP BY bp_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.92c9353a-8f43-4456-9011-e27be21e9a64"),
    TnI_labs=Input(rid="ri.vector.main.execute.1818dfe8-1d8a-43b6-9de7-bcff230b57b8")
)
SELECT  min(tn_date) as first_TnI,
        first_value(tn_patient) as TnI_ID,   
        first_value(tn_value) as TnI_level,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status  
FROM TnI_labs
GROUP BY tn_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.76ea253c-7104-446f-a476-55165a272bbc"),
    TnT_labs=Input(rid="ri.vector.main.execute.1a9af0be-71e8-404b-816f-960673b62169")
)
SELECT  min(tn_date) as first_TnT,
        first_value(tn_patient) as TnT_ID,   
        first_value(tn_value) as TnT_level,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status 
FROM TnT_labs
GROUP BY tn_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.5feccb73-b9bf-4532-9111-5418d2211f43"),
    Tn_unk_labs=Input(rid="ri.vector.main.execute.661f9c19-4a69-4b15-9bfb-f14d32befeda")
)
SELECT  min(tn_date) as first_Tn_unk,
        first_value(tn_patient) as Tn_unk_ID,   
        first_value(tn_value) as Tn_unk_level    
FROM Tn_unk_labs
GROUP BY tn_patient

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1d76a5fe-d0de-4007-9360-069ed05808e1"),
    obese=Input(rid="ri.vector.main.execute.8fab78b6-1f6b-42df-8c27-21ed9dba5698")
)
SELECT  min(obese_start) as first_obese_date,
        first_value(obese_id) as obese_patient,
        first_value(obese_descrip) as first_obese_descrip
FROM obese
group by obese_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.1dff3deb-eac6-4d43-9b4f-ca9f1ea080b3"),
    proBNP_labs=Input(rid="ri.vector.main.execute.a1a985a1-f750-4b6d-84be-147e488c4177")
)
SELECT  min(bnp_date) as first_proBNP,
        first_value(bnp_patient) as proBNP_ID,   
        first_value(bnp_value) as proBNP_level,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status        
FROM proBNP_labs
GROUP BY bnp_patient

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8598c255-b4c6-4edc-8c74-3e811b17c31d"),
    smoking=Input(rid="ri.vector.main.execute.8bf6368e-c896-454e-9a04-28299e4410b2")
)
SELECT  min(smoke_start) as first_smoke_date,
        first_value(smoke_id) as smoke_patient        
FROM smoking
group by smoke_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.e9ec7d30-5cb3-4290-a22c-c3b841853993"),
    statin_prior=Input(rid="ri.vector.main.execute.c78f718e-2d05-40b6-8596-09524fa66575")
)
SELECT  min(statin_start) as first_statin_date,
        first_value(statin_patient) as statin_ID,
        (statin_patient / statin_patient) as statin_prior                
FROM statin_prior
group by statin_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.6f8dc3df-a4c3-4eeb-b495-8b3fd098ceb6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  condition_concept_name as hf     
FROM condition_occurrence
Where (condition_concept_name like '%heart failure%' or condition_concept_name like '%Heart failure%')
        and condition_concept_name not like '%Hypertensive heart disease without%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.0f9cfa96-5aaa-4ac5-b1f0-0a60e7a8853b"),
    first_BNP=Input(rid="ri.vector.main.execute.e876cb8b-89b0-4896-a299-3608993793ac")
)
SELECT *
FROM first_BNP
WHERE BNP_level >100

@transform_pandas(
    Output(rid="ri.vector.main.execute.b84dcef7-7198-4331-9e84-7d5b8e2a4627"),
    hi_NP_patient=Input(rid="ri.vector.main.execute.e2bfbe5b-d6c2-41ed-9af0-106975eade88")
)
select  c19_status, 
        count(1) as num_records,        
        avg(HF_status) as HFnum     
from hi_NP_patient
group by c19_status, HF_status
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.b3976f27-926c-4bb7-8247-36cee65c57f9"),
    BNP_models=Input(rid="ri.vector.main.execute.3e925583-4eff-4b96-8444-a3667614a94d")
)
SELECT *
FROM BNP_models
WHERE hi_NP_ID is null

@transform_pandas(
    Output(rid="ri.vector.main.execute.e2bfbe5b-d6c2-41ed-9af0-106975eade88"),
    hi_NPs=Input(rid="ri.vector.main.execute.1bfe0c81-4570-40dc-b12c-0981fa6fe210")
)
SELECT  min(bnp_date) as hi_NP_date,
        first_value(bnp_patient) as hi_NP_ID,   
        first_value(bnp_type) as hi_NP_type,
        first_value(c19) as c19_status,
        first_value(HFcase) as HF_status
FROM hi_NPs
GROUP BY bnp_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.1bfe0c81-4570-40dc-b12c-0981fa6fe210"),
    bnp_data=Input(rid="ri.foundry.main.dataset.ef050a76-d4ea-4fb3-9ae3-f732cdf505f5")
)
SELECT *
FROM bnp_data
WHERE (bnp_type not like '%prohormone%' and bnp_value >100) or (bnp_type like '%prohormone%' and bnp_value >300)

@transform_pandas(
    Output(rid="ri.vector.main.execute.dcfe8e36-ef97-454a-889d-8ecfc721b874"),
    first_proBNP=Input(rid="ri.vector.main.execute.1dff3deb-eac6-4d43-9b4f-ca9f1ea080b3")
)
SELECT *
FROM first_proBNP
WHERE proBNP_level >300

@transform_pandas(
    Output(rid="ri.vector.main.execute.45874bba-5b90-4462-af9d-3b21eac25db1"),
    hosp_c19status=Input(rid="ri.vector.main.execute.95f4aa49-503b-4092-8ad0-bdd2b704ad96")
)
SELECT *
FROM hosp_c19status
WHERE first_diagnosis_date is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.95f4aa49-503b-4092-8ad0-bdd2b704ad96"),
    Covid_positive_persons_De_identified_=Input(rid="ri.foundry.main.dataset.f95dc2b0-4c06-41df-9d72-a186f94e5719"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.89927e78-e712-4dcd-a470-18c1620bd03e")
)
SELECT microvisits_to_macrovisits.person_id as patient_id1, 
    microvisits_to_macrovisits.macrovisit_id, 
    microvisits_to_macrovisits.macrovisit_start_date, 
    microvisits_to_macrovisits.macrovisit_end_date, 
    Covid_positive_persons_De_identified_.covid_event_type, 
    Covid_positive_persons_De_identified_.first_diagnosis_date, 
    Covid_positive_persons_De_identified_.person_id as patient_id2
FROM microvisits_to_macrovisits left join Covid_positive_persons_De_identified_
ON microvisits_to_macrovisits.person_id = Covid_positive_persons_De_identified_.person_id
WHERE microvisits_to_macrovisits.macrovisit_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.d35e42cf-334c-493c-8d03-41f405d44bed"),
    hosp_c19status=Input(rid="ri.vector.main.execute.95f4aa49-503b-4092-8ad0-bdd2b704ad96")
)
SELECT *
FROM hosp_c19status
WHERE first_diagnosis_date is null

@transform_pandas(
    Output(rid="ri.vector.main.execute.6d2cbe70-bed8-4d26-8a3c-30acc710e082"),
    hosp_c19=Input(rid="ri.vector.main.execute.45874bba-5b90-4462-af9d-3b21eac25db1")
)
SELECT *
FROM hosp_c19
WHERE first_diagnosis_date between macrovisit_start_date and macrovisit_end_date

@transform_pandas(
    Output(rid="ri.vector.main.execute.73121a1f-9536-495f-ad50-1c51ad0391a0"),
    index_c19=Input(rid="ri.vector.main.execute.6d2cbe70-bed8-4d26-8a3c-30acc710e082")
)
SELECT  min(index_c19.macrovisit_start_date) as index_admit,
        first_value(index_c19.patient_id1) as patient_id,
        first_value(index_c19.macrovisit_id) as macrovisit_num,
        first_value(index_c19.macrovisit_end_date) as index_dc,
        first_value(index_c19.covid_event_type) as covid_dx,
        first_value(index_c19.first_diagnosis_date) as covid_dx_date      
FROM index_c19
group by patient_id1

@transform_pandas(
    Output(rid="ri.vector.main.execute.3c4a350c-6048-44c6-90a2-d5748adff9ad"),
    hosp_no_c19=Input(rid="ri.vector.main.execute.d35e42cf-334c-493c-8d03-41f405d44bed")
)
SELECT  min(hosp_no_c19.macrovisit_start_date) as index_admit,
        first_value(hosp_no_c19.patient_id1) as patient_id,
        first_value(hosp_no_c19.macrovisit_id) as macrovisit_num,
        first_value(hosp_no_c19.macrovisit_end_date) as index_dc        
FROM hosp_no_c19
where macrovisit_start_date >= '2020-03-01'
group by patient_id1

@transform_pandas(
    Output(rid="ri.vector.main.execute.972341f9-b4d4-4ea3-8925-89f92bffa596"),
    first_CAD=Input(rid="ri.foundry.main.dataset.9982d9a0-564f-49d1-a370-14b8b1f09d19"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  noC19_IRtime_update.patient_id,
        first_CAD.CAD_patient,
         case when ((first_CAD.first_CAD_date is null) or (noC19_IRtime_update.index_dc < first_CAD.first_CAD_date)) then 0 else 1 end as CAD_hx         
FROM noC19_IRtime_update left join first_CAD
on noC19_IRtime_update.patient_id = first_CAD.CAD_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.d0a8b28d-2d2f-4c2c-87e5-aa9c832d817e"),
    noC19_CAD=Input(rid="ri.vector.main.execute.972341f9-b4d4-4ea3-8925-89f92bffa596")
)
SELECT CAD_hx,
        count(1) as CADpatient
FROM noC19_CAD
group by CAD_hx

@transform_pandas(
    Output(rid="ri.vector.main.execute.6660254d-a531-4da9-ab42-3d931e187a6c"),
    noC19_CAD=Input(rid="ri.vector.main.execute.972341f9-b4d4-4ea3-8925-89f92bffa596"),
    noC19_obese=Input(rid="ri.vector.main.execute.bd4e5971-3138-4dd4-a438-9cac869516a8")
)
SELECT  noC19_obese.patient_id,
        noC19_obese.obese_hx,
        noC19_CAD.CAD_hx
FROM noC19_CAD inner join noC19_obese
on noC19_CAD.patient_id = noC19_obese.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.bcfd70d3-8624-40aa-9f3d-09f35d85c045"),
    noC19_CADobese=Input(rid="ri.vector.main.execute.6660254d-a531-4da9-ab42-3d931e187a6c"),
    noC19_CKD=Input(rid="ri.vector.main.execute.81e34bf4-2ea4-42d8-b14b-3229c4d2b2c7")
)
SELECT  noC19_CADobese.patient_id,
        noC19_CADobese.CAD_hx,
        noC19_CADobese.obese_hx,
        noC19_CKD.CKD_hx
FROM noC19_CADobese inner join noC19_CKD
on noC19_CADobese.patient_id = noC19_CKD.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.81e34bf4-2ea4-42d8-b14b-3229c4d2b2c7"),
    first_CKD=Input(rid="ri.foundry.main.dataset.7dc5eb1b-8580-42e5-9b3a-81d64ced7431"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  noC19_IRtime_update.patient_id,
        first_CKD.CKD_patient,
        case when ((first_CKD.first_CKD_date is null) or (noC19_IRtime_update.index_dc < first_CKD.first_CKD_date)) then 0 else 1 end as CKD_hx        
FROM noC19_IRtime_update left join first_CKD
on noC19_IRtime_update.patient_id = first_CKD.CKD_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.57ecae7c-c3a4-4d57-9bc1-eed12398ef81"),
    noC19_CKD=Input(rid="ri.vector.main.execute.81e34bf4-2ea4-42d8-b14b-3229c4d2b2c7")
)
SELECT  CKD_hx,
        count(1) as CKDpatient
FROM noC19_CKD
group by CKD_hx

@transform_pandas(
    Output(rid="ri.vector.main.execute.388a6135-6cfa-4cd8-aebd-baa8de14fe2f"),
    first_COPD=Input(rid="ri.foundry.main.dataset.6feae61c-75ec-4e9b-81a3-4ddfb0662b4b"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  noC19_IRtime_update.patient_id,
        first_COPD.COPD_patient,
         case when ((first_COPD.first_COPD_date is null) or (noC19_IRtime_update.index_dc < first_COPD.first_COPD_date)) then 0 else 1 end as COPD_hx          
FROM noC19_IRtime_update left join first_COPD
on noC19_IRtime_update.patient_id = first_COPD.COPD_patient
 

@transform_pandas(
    Output(rid="ri.vector.main.execute.5bd89996-9476-4075-b03d-dcc96caef15f"),
    noC19_COPD=Input(rid="ri.vector.main.execute.388a6135-6cfa-4cd8-aebd-baa8de14fe2f")
)
SELECT COPD_hx,
        count(1) as COPDpatient
FROM noC19_COPD
group by COPD_hx

@transform_pandas(
    Output(rid="ri.vector.main.execute.16284b13-9154-4c03-9682-162619695833"),
    first_DM=Input(rid="ri.foundry.main.dataset.bbc5a5ab-0870-4e6d-baf5-d4d1d962ef26"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  noC19_IRtime_update.patient_id,
        noC19_IRtime_update.HF,              
        first_DM.DM_patient,
        case when ((first_DM.first_DM_date is null) or (noC19_IRtime_update.index_dc < first_DM.first_DM_date)) then 0 else 1 end as DM_hx 
FROM noC19_IRtime_update left join first_DM
on noC19_IRtime_update.patient_id = first_DM.DM_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.5eba00ba-9e43-494a-b0e0-1431cd4ed766"),
    noC19_DM=Input(rid="ri.vector.main.execute.16284b13-9154-4c03-9682-162619695833")
)
SELECT  DM_hx,
        count(1) as DMpatient
FROM noC19_DM
group by DM_hx

@transform_pandas(
    Output(rid="ri.vector.main.execute.fd80efb3-c450-4627-89a5-b06be5da7812"),
    noC19_fu_parent_update=Input(rid="ri.foundry.main.dataset.40d37729-ea0d-4d7e-9a29-aba284657e8b")
)
SELECT  patient_id as DeathNoHF_ID,
        datediff (death_date, index_dc) as fuDeath_NoHF        
FROM noC19_fu_parent_update
WHERE (HF_id is null) and (death_date is not null) 

@transform_pandas(
    Output(rid="ri.vector.main.execute.5a46d358-d1f3-49e9-a1a2-810f4c1c5385"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  HF,
        futime_HF,
        case when (futime_HF <=30) then 1 else 0 end as HF30         
FROM noC19_IRtime_update
WHERE HF is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.1ca83170-2e05-4b6a-800b-93a9499e28eb"),
    noC19_HF30=Input(rid="ri.vector.main.execute.5a46d358-d1f3-49e9-a1a2-810f4c1c5385")
)
SELECT  sum(HF),
        sum(HF30)
FROM noC19_HF30

@transform_pandas(
    Output(rid="ri.vector.main.execute.74ff50dd-09f3-4170-a43b-7e17afb3cde6"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  first_HF_descrip,
        count(*)
FROM noC19_IRtime
Where HF is not null
Group by first_HF_descrip

@transform_pandas(
    Output(rid="ri.vector.main.execute.25567574-4957-4f0e-b601-d5235d41bc45"),
    first_HTN=Input(rid="ri.foundry.main.dataset.13b4990e-0a61-45db-8369-3baec3b00bd4"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  noC19_IRtime_update.patient_id,
        first_HTN.HTN_patient,
        case when ((first_HTN.first_HTN_date is null) or (noC19_IRtime_update.index_dc < first_HTN.first_HTN_date)) then 0 else 1 end as HTN_hx         
FROM noC19_IRtime_update left join first_HTN
on noC19_IRtime_update.patient_id = first_HTN.HTN_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.c09e79ad-a0db-4e63-b2e9-9f36b69575d3"),
    noC19_HTN=Input(rid="ri.vector.main.execute.25567574-4957-4f0e-b601-d5235d41bc45")
)
SELECT HTN_hx,
        count(1) as HTNpatient
FROM noC19_HTN
group by HTN_hx

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b"),
    noC19_demfu=Input(rid="ri.vector.main.execute.f27ede30-1cb7-4f4b-a880-e8c1d3907929")
)
SELECT *,
        (futime_DeathNoHF + futime_NoEvent) as futime_NoHF,
        (futime_DeathNoHF + futime_NoEvent + futime_HF) as futime_all,
        (HF_id / HF_id) as HF,
        0 as c19
FROM noC19_demfu

@transform_pandas(
    Output(rid="ri.vector.main.execute.13da7b0c-4b0a-4694-84cf-d8e08c12c06c"),
    noC19_fu_parent_update=Input(rid="ri.foundry.main.dataset.40d37729-ea0d-4d7e-9a29-aba284657e8b")
)
SELECT  patient_id as noevent_id,
        datediff (surv_end_date, index_dc) as fuNoEvent 
FROM noC19_fu_parent_update
WHERE (death_date is null) and (HF_id is null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.822648fb-0fa7-4e30-9d3d-7695d25288de"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT ethnicity,
        count(*)
FROM noC19_IRtime_update
WHERE race like '%null%' or race like '%Unknown%' or race like '%No information%' or race like '%Refuse%' or race like '%No matching%'
group by ethnicity

@transform_pandas(
    Output(rid="ri.vector.main.execute.0b1f3a73-1e85-42fb-bf42-2be7ea57e1cf"),
    noC19_CADobeseCKD=Input(rid="ri.vector.main.execute.bcfd70d3-8624-40aa-9f3d-09f35d85c045"),
    noC19_DM=Input(rid="ri.vector.main.execute.16284b13-9154-4c03-9682-162619695833")
)
SELECT  noC19_CADobeseCKD.patient_id,
        noC19_CADobeseCKD.CAD_hx,
        noC19_CADobeseCKD.obese_hx,
        noC19_CADobeseCKD.CKD_hx,
        noC19_DM.DM_hx
FROM noC19_CADobeseCKD inner join noC19_DM
on noC19_CADobeseCKD.patient_id = noC19_DM.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.2329abbd-dc9a-4dbc-8dbf-d432b5c39836"),
    noC19_COPD=Input(rid="ri.vector.main.execute.388a6135-6cfa-4cd8-aebd-baa8de14fe2f"),
    noC19_comorbid=Input(rid="ri.vector.main.execute.0b1f3a73-1e85-42fb-bf42-2be7ea57e1cf")
)
SELECT  noC19_comorbid.patient_id,
        noC19_comorbid.CAD_hx,
        noC19_comorbid.obese_hx,
        noC19_comorbid.CKD_hx,
        noC19_comorbid.DM_hx,
        noC19_COPD.COPD_hx
FROM noC19_comorbid inner join noC19_COPD
on noC19_comorbid.patient_id = noC19_COPD.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.0dd0d213-d800-47b7-84c1-3d11927352ed"),
    noC19_comorbid=Input(rid="ri.vector.main.execute.0b1f3a73-1e85-42fb-bf42-2be7ea57e1cf")
)
SELECT  count(ID),
        count(distinct ID)
FROM noC19_comorbid

@transform_pandas(
    Output(rid="ri.vector.main.execute.224d1dba-393e-4f19-b99f-cc40d20f7ed3"),
    noC19_HTN=Input(rid="ri.vector.main.execute.25567574-4957-4f0e-b601-d5235d41bc45"),
    noC19_comorbid_COPD=Input(rid="ri.vector.main.execute.2329abbd-dc9a-4dbc-8dbf-d432b5c39836")
)
SELECT  noC19_comorbid_COPD.patient_id,
        noC19_comorbid_COPD.CAD_hx,
        noC19_comorbid_COPD.obese_hx,
        noC19_comorbid_COPD.CKD_hx,
        noC19_comorbid_COPD.DM_hx,
        noC19_comorbid_COPD.COPD_hx,
        noC19_HTN.HTN_hx
FROM noC19_comorbid_COPD inner join noC19_HTN
on noC19_comorbid_COPD.patient_id = noC19_HTN.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.2b68f388-4f63-4edb-8929-3b6b92349b56"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  count(patient_id),
        count(distinct patient_id),
        count(HF_id),
        count(HF),
        count(DeathNoHF_ID),
        count(noevent_id),
        count(death_id),
        percentile_approx(fuHF, 0.5) as HFmedian,
        percentile_approx(fuHF, 0.25) as HFq1,
        percentile_approx(fuHF, 0.75) as HFq3,
        percentile_approx(fuDeath, 0.5) as Deathmedian,
        percentile_approx(fuDeath, 0.25) as Deathq1,
        percentile_approx(fuDeath, 0.75) as Deathq3,
        percentile_approx(futime_all, 0.5) as Allmedian,
        percentile_approx(futime_all, 0.25) as Allq1,
        percentile_approx(futime_all, 0.75) as Allq3,
        sum(futime_HF),
        sum(futime_DeathNoHF),
        sum(futime_NoEvent),
        sum(futime_NoHF),
        sum(futime_Death),
        sum(futime_all),
        avg(fuHF),
        avg(fuDeath),
        avg(fuDeath_NoHF),
        avg(futime_NoEvent),
        avg(futime_all)       
FROM noC19_IRtime_update

@transform_pandas(
    Output(rid="ri.vector.main.execute.24d8dcad-f0c8-4492-bc52-4878bc0307fd"),
    deathfile=Input(rid="ri.vector.main.execute.5cb733b6-7e55-488d-9f8f-8d337fc1a53a"),
    no_C19_noHxHF=Input(rid="ri.vector.main.execute.ab0a84bf-2bc4-46e7-b71d-d476cb6d6491")
)
SELECT *
FROM no_C19_noHxHF left join deathfile
on no_C19_noHxHF.patient_id = deathfile.death_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.2895dd79-7f3b-4a31-a5cb-5179652ecacd"),
    dems=Input(rid="ri.vector.main.execute.38b9ca54-d851-41f6-b4f9-09a683075ca2"),
    noC19_fu=Input(rid="ri.vector.main.execute.ca3eb814-0a16-4cd5-9a9a-35a2f3183f29")
)
SELECT *
FROM noC19_fu left join dems
on noC19_fu.patient_id = dems.dem_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.f27ede30-1cb7-4f4b-a880-e8c1d3907929"),
    noC19_demNoAge=Input(rid="ri.vector.main.execute.2895dd79-7f3b-4a31-a5cb-5179652ecacd")
)
SELECT *,
        (2022 - birthyear) as age,
        coalesce(fuHF, 0) as futime_HF,
        coalesce(fuDeath_NoHF, 0) as futime_DeathNoHF,
        coalesce(fuNoEvent, 0) as futime_NoEvent,
        coalesce(fuDeath, 0) as futime_Death
FROM noC19_demNoAge

@transform_pandas(
    Output(rid="ri.vector.main.execute.016ed500-152d-41c6-9b40-aba9a18c5c98"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT ethnicity,
        count(*)
FROM noC19_IRtime
group by ethnicity

@transform_pandas(
    Output(rid="ri.vector.main.execute.ca3eb814-0a16-4cd5-9a9a-35a2f3183f29"),
    noC19_DeathNoHF=Input(rid="ri.vector.main.execute.fd80efb3-c450-4627-89a5-b06be5da7812"),
    noC19_parent_and_NoEvent=Input(rid="ri.vector.main.execute.0037d4d7-8a2a-43f9-a0c7-b86b98ed9a6a")
)
SELECT *
FROM noC19_parent_and_NoEvent left join noC19_DeathNoHF
on noC19_parent_and_NoEvent.patient_id = noC19_DeathNoHF.DeathNoHF_ID

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.40d37729-ea0d-4d7e-9a29-aba284657e8b"),
    noC19_postDC=Input(rid="ri.vector.main.execute.b275b56f-2a5c-4452-952e-22b9db892566")
)
SELECT  *,
        datediff (first_HF_date, index_dc) as fuHF,
        datediff (death_date, index_dc) as fuDeath,        
        coalesce(death_date, '2022-03-23') as surv_end_date      
FROM noC19_postDC

@transform_pandas(
    Output(rid="ri.vector.main.execute.c0d3e816-2618-4555-af30-420a1a9ce4d3"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  avg(age),
        stddev_samp(age)
FROM noC19_IRtime_update

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1643de5a-9154-48e2-97c4-59043804a4e7"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b"),
    noC19_comorbid_all=Input(rid="ri.vector.main.execute.224d1dba-393e-4f19-b99f-cc40d20f7ed3")
)
SELECT  noC19_IRtime_update.index_dc,
        noC19_IRtime_update.sex,
        noC19_IRtime_update.race,
        noC19_IRtime_update.ethnicity,
        noC19_IRtime_update.age,
        noC19_IRtime_update.futime_all,
        noC19_IRtime_update.futime_Death,
        (noC19_IRtime_update.DeathNoHF_ID / noC19_IRtime_update.DeathNoHF_ID) as DeathNoHF,
        (noC19_IRtime_update.death_id / noC19_IRtime_update.death_id) as Death,
        (noC19_IRtime_update.futime_HF + noC19_IRtime_update.futime_DeathNoHF + noC19_IRtime_update.futime_NoEvent) as futime_Comp,
        case when (noC19_IRtime_update.DeathNoHF_ID is null and noC19_IRtime_update.HF is null) then 0 else 1 end as comp_case,
        noC19_IRtime_update.macrovisit_num,
        coalesce (noC19_IRtime_update.HF, 0) as HFcase,        
        noC19_IRtime_update.c19,
        noC19_comorbid_all.patient_id,
        noC19_comorbid_all.CAD_hx,
        noC19_comorbid_all.obese_hx,
        noC19_comorbid_all.CKD_hx,
        noC19_comorbid_all.DM_hx,
        noC19_comorbid_all.COPD_hx,
        noC19_comorbid_all.HTN_hx
FROM noC19_IRtime_update inner join noC19_comorbid_all
on noC19_IRtime_update.patient_id = noC19_comorbid_all.patient_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.c522b92b-b22f-4c8f-9ccc-298f21cce6c0"),
    noC19_multivar=Input(rid="ri.foundry.main.dataset.1643de5a-9154-48e2-97c4-59043804a4e7")
)
SELECT count(patient_id)
FROM noC19_multivar
where age is null

@transform_pandas(
    Output(rid="ri.vector.main.execute.8af5638e-0f5d-4445-aec3-6da0e093731c"),
    no_C19_noHxHF=Input(rid="ri.vector.main.execute.ab0a84bf-2bc4-46e7-b71d-d476cb6d6491")
)
SELECT  count(patient_id),
        count(distinct patient_id)
FROM no_C19_noHxHF

@transform_pandas(
    Output(rid="ri.vector.main.execute.bd4e5971-3138-4dd4-a438-9cac869516a8"),
    first_obese=Input(rid="ri.foundry.main.dataset.1d76a5fe-d0de-4007-9360-069ed05808e1"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  noC19_IRtime_update.patient_id,
        first_obese.obese_patient,
        case when ((first_obese.first_obese_date is null) or (noC19_IRtime_update.index_dc < first_obese.first_obese_date)) then 0 else 1 end as obese_hx           
FROM noC19_IRtime_update left join first_obese
on noC19_IRtime_update.patient_id = first_obese.obese_patient

@transform_pandas(
    Output(rid="ri.vector.main.execute.3453adaf-5521-4dc3-8b79-f89fed11bfa1"),
    noC19_obese=Input(rid="ri.vector.main.execute.bd4e5971-3138-4dd4-a438-9cac869516a8")
)
SELECT obese_hx,
        count(1) as obesepatient
FROM noC19_obese
group by obese_hx

@transform_pandas(
    Output(rid="ri.vector.main.execute.0037d4d7-8a2a-43f9-a0c7-b86b98ed9a6a"),
    noC19_NOevent_time=Input(rid="ri.vector.main.execute.13da7b0c-4b0a-4694-84cf-d8e08c12c06c"),
    noC19_fu_parent_update=Input(rid="ri.foundry.main.dataset.40d37729-ea0d-4d7e-9a29-aba284657e8b")
)
SELECT *
FROM noC19_fu_parent_update left join noC19_NOevent_time
on noC19_fu_parent_update.patient_id = noC19_NOevent_time.noevent_id

@transform_pandas(
    Output(rid="ri.vector.main.execute.b275b56f-2a5c-4452-952e-22b9db892566"),
    noC19_death_outcome=Input(rid="ri.vector.main.execute.24d8dcad-f0c8-4492-bc52-4878bc0307fd")
)
SELECT *
FROM noC19_death_outcome
where (index_dc < death_date) or (death_date is null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.83f2daf9-d71a-45f4-be0a-27b7a576754b"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT race,
        count(*)
FROM noC19_IRtime_update
group by race

@transform_pandas(
    Output(rid="ri.vector.main.execute.2ca8613c-4629-4a3f-9ecd-d952167a304e"),
    index_no_c19=Input(rid="ri.vector.main.execute.3c4a350c-6048-44c6-90a2-d5748adff9ad")
)
SELECT *
FROM index_no_c19
tablesample (15 percent)

@transform_pandas(
    Output(rid="ri.vector.main.execute.82920786-c094-4ba1-887e-86e6de22f220"),
    noC19_sample=Input(rid="ri.vector.main.execute.2ca8613c-4629-4a3f-9ecd-d952167a304e")
)
SELECT  count(patient_id),
        count(distinct patient_id)
FROM noC19_sample

@transform_pandas(
    Output(rid="ri.vector.main.execute.017c3ad9-2f61-4c07-b9a7-38e1a9060a97"),
    noC19_IRtime_update=Input(rid="ri.foundry.main.dataset.d89d4a69-6f43-4416-8cc4-e9a5bb96fa0b")
)
SELECT  sex,
        count(*)
FROM noC19_IRtime_update
Group by sex

@transform_pandas(
    Output(rid="ri.vector.main.execute.ab0a84bf-2bc4-46e7-b71d-d476cb6d6491"),
    first_HF=Input(rid="ri.vector.main.execute.c39feb8b-b0af-441f-b696-2e75f71302ec"),
    noC19_sample=Input(rid="ri.vector.main.execute.2ca8613c-4629-4a3f-9ecd-d952167a304e")
)
SELECT *
FROM noC19_sample left join first_HF
on noC19_sample.patient_id = first_HF.HF_id
where (noC19_sample.index_dc < first_HF.first_HF_date) or (first_HF.first_HF_date is null)

@transform_pandas(
    Output(rid="ri.vector.main.execute.9477f216-3d27-4c44-a5fd-360a2ecebd14"),
    index_no_c19=Input(rid="ri.vector.main.execute.3c4a350c-6048-44c6-90a2-d5748adff9ad")
)
SELECT  count(patient_id),
        count(distinct patient_id)
FROM index_no_c19

@transform_pandas(
    Output(rid="ri.vector.main.execute.be9a5241-c4c4-4795-912e-aab9502d2724"),
    noC19_COPD=Input(rid="ri.vector.main.execute.388a6135-6cfa-4cd8-aebd-baa8de14fe2f")
)
SELECT count(patient_id)
FROM noC19_COPD
where COPD_patient is null

@transform_pandas(
    Output(rid="ri.vector.main.execute.8fab78b6-1f6b-42df-8c27-21ed9dba5698"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as obese_id, 
        condition_concept_name as obese_descrip,
        condition_start_date as obese_start      
FROM condition_occurrence
Where condition_concept_name like '%obes%' or condition_concept_name like '%Obes%'

@transform_pandas(
    Output(rid="ri.vector.main.execute.a1a985a1-f750-4b6d-84be-147e488c4177"),
    bnp_data=Input(rid="ri.foundry.main.dataset.ef050a76-d4ea-4fb3-9ae3-f732cdf505f5")
)
SELECT *
FROM bnp_data 
WHERE (bnp_data.bnp_type like '%prohormone%') and bnp_value is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.90b5981d-dd27-45bf-b655-5db73699bb17"),
    first_proBNP=Input(rid="ri.vector.main.execute.1dff3deb-eac6-4d43-9b4f-ca9f1ea080b3")
)
select  c19_status, 
        count(1) as num_records,
        percentile_approx(proBNP_level, 0.5)  as median,
        percentile_approx(proBNP_level, 0.25) as q1,
        percentile_approx(proBNP_level, 0.75) as q3         
from first_proBNP
group by c19_status
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.e7a21e82-bd0a-471d-bf68-02cdc3f7e8c5"),
    data_all=Input(rid="ri.foundry.main.dataset.affd8c40-b8a7-4a79-b931-da39819027b0")
)
SELECT  patient_id as racegrp_id,
        race,
        ethnicity,
        case when ((race is null or race like '%No%' or race like '%Unknown%' or race like '%Refuse%') and ethnicity not like '%Hispanic%') then 0 else 1 end as racegrp
FROM data_all

@transform_pandas(
    Output(rid="ri.vector.main.execute.3842ffa5-dc9f-4b08-bdea-b80bc174b77c"),
    first_smoke=Input(rid="ri.foundry.main.dataset.8598c255-b4c6-4edc-8c74-3e811b17c31d")
)
SELECT *,
        (smoke_patient / smoke_patient) as smoking
FROM first_smoke

@transform_pandas(
    Output(rid="ri.vector.main.execute.8bf6368e-c896-454e-9a04-28299e4410b2"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.526c0452-7c18-46b6-8a5d-59be0b79a10b")
)
SELECT  person_id as smoke_id, 
        condition_concept_name as smoke_descrip,
        condition_start_date as smoke_start      
FROM condition_occurrence
Where condition_concept_name like '%tobacco%' or condition_concept_name like '%Tobacco%' or condition_concept_name like '%Smok%' or
condition_concept_name like '%smoke%' 

@transform_pandas(
    Output(rid="ri.vector.main.execute.eedb0f4c-8023-45b7-8fcc-aed0bb4b5da5"),
    drug_era=Input(rid="ri.foundry.main.dataset.4f424984-51a6-4b10-9b2b-0410afa1b2f8")
)
SELECT  person_id,
        drug_concept_name,
        drug_era_start_date
FROM drug_era
WHERE drug_concept_name like '%statin%' 

@transform_pandas(
    Output(rid="ri.vector.main.execute.2045e3f4-1620-4448-adfb-16e6cb9c5bd5"),
    statin_prior=Input(rid="ri.vector.main.execute.c78f718e-2d05-40b6-8596-09524fa66575")
)
select  c19,
        count(1) as num_records ,       
        count(distinct statin_patient)       
from statin_prior
group by c19
order by num_records asc

@transform_pandas(
    Output(rid="ri.vector.main.execute.c78f718e-2d05-40b6-8596-09524fa66575"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d"),
    statin=Input(rid="ri.vector.main.execute.eedb0f4c-8023-45b7-8fcc-aed0bb4b5da5")
)
SELECT  statin.person_id as statin_patient,
        statin.drug_concept_name as statins,
        statin.drug_era_start_date as statin_start,
       dataset_all_update.c19,
       dataset_all_update.HFcase
FROM dataset_all_update left join statin
ON dataset_all_update.patient_id = statin.person_id
WHERE statin.drug_era_start_date <= dataset_all_update.index_dc and statin.person_id is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.878498d5-3a01-4815-adab-6e92f85a8db0"),
    dataset_all_update=Input(rid="ri.foundry.main.dataset.426d31d3-0c08-44cb-8ab6-3fc00fb57b3d"),
    tn_macrovisit=Input(rid="ri.vector.main.execute.1655d6c8-8dd2-41d8-b8cd-6cef2c2599e8")
)
SELECT  tn_macrovisit.tn_patient,
        tn_macrovisit.tn_value,
        tn_macrovisit.tn_date,
        tn_macrovisit.tn_type,
        dataset_all_update.c19,
        dataset_all_update.HFcase
FROM dataset_all_update left join tn_macrovisit
ON dataset_all_update.macrovisit_num = tn_macrovisit.macrovisit_id
WHERE tn_macrovisit.tn_patient is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.57cbde05-b581-485f-997b-21900342b8eb"),
    measurement=Input(rid="ri.foundry.main.dataset.29834e2c-f924-45e8-90af-246d29456293")
)
SELECT  person_id as tn_patient,
        measurement_id as tn_measurement_id,
        measurement_concept_name as tn_type,
        harmonized_value_as_number as tn_value,
        measurement_date as tn_date
FROM measurement
WHERE (measurement_concept_name like '%troponin%' or measurement_concept_name like '%Troponin%') and harmonized_value_as_number is not null and measurement_id is not null

@transform_pandas(
    Output(rid="ri.vector.main.execute.1655d6c8-8dd2-41d8-b8cd-6cef2c2599e8"),
    measurements_to_macrovisits=Input(rid="ri.foundry.main.dataset.7d655791-94cc-4322-b9e9-ce66308126f5"),
    tn_labs=Input(rid="ri.vector.main.execute.57cbde05-b581-485f-997b-21900342b8eb")
)
SELECT  tn_labs.tn_patient,
        tn_labs.tn_date,
        tn_labs.tn_type,
        tn_labs.tn_value,
        measurements_to_macrovisits.macrovisit_id
FROM tn_labs left join measurements_to_macrovisits 
ON tn_labs.tn_measurement_id = measurements_to_macrovisits.measurement_id  
WHERE measurements_to_macrovisits.macrovisit_id is not null 

@transform_pandas(
    Output(rid="ri.vector.main.execute.e87a6e42-66b9-489d-a802-454f98dbf3fc"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT *,
        
FROM dataset_all_Rx

@transform_pandas(
    Output(rid="ri.vector.main.execute.880cde23-f090-41ab-b659-5f3f887b2ac6"),
    dataset_all_Rx=Input(rid="ri.foundry.main.dataset.41b598e7-d6cc-4d6f-986c-860007e8ed85")
)
SELECT  percentile_approx(futime_all, 0.5) as Allmedian,
        percentile_approx(futime_all, 0.25) as Allq1,
        percentile_approx(futime_all, 0.75) as Allq3
FROM dataset_all_Rx

@transform_pandas(
    Output(rid="ri.vector.main.execute.14a5b1b0-0a6c-4af6-8014-23591498c251"),
    race_cc=Input(rid="ri.vector.main.execute.e7a21e82-bd0a-471d-bf68-02cdc3f7e8c5")
)
SELECT  racegrp_id,
        case when race = 'White' then 1 else 0 end as white
FROM race_cc
where racegrp = 1

