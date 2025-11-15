# Datasets
## WeDar
From first glance, it looks like the data is for single location point?, do we not have a complete data  (with whether data for all lat and lon points of germany) for each day with time information
## MaStr
Data understanding needed (perform EDA)
## ENTSO-E

Where this data would be needed?

# Tasks
## 1.  The infeed of renewable power based on weather forecast

#### **Inputs:**
- Weather data (**WeDar**)
- Energy asset registry (**MaStr**)
#### **Goal:**  
- Predict how much power would, wind, PV assets inject into the grid over time on a **2×2 km grid** in germany
- Hourly forecasts for **0–48 hours ahead** (at every hour).
#### Questions:
1. Is **hourly** time step and **0–48 h** horizon, OK?
2. What **update cadence** should we use (e.g., refresh forecasts every **6 h** with new weather runs)?
3. Is there any **official 2×2 km grid definition** (shapefile/GeoJSON, CRS, extent) or I can create one by myself?
4. Could you clarify what “including confidence level (p-value, etc.)” mean? 
	- Do you mean **prediction intervals** per timestep/cell (e.g., **80%** or **90%** bands)? or when the actual value y*t* arrives, we don’t compute “confidence” retroactively; instead we **evaluate** the forecast’s accuracy and calibration, Is that the approach you’d like?
	- My understanding is that, in energy forecasting, we usually report **prediction intervals** rather than **p-values**
5. Do we have any data for **actual feed-in**, with **geographic granularity** of quadrant? If not, how are we suppose to do question 4?


## 2.  Spatial infeed distribution across Germany over the year
#### **Inputs:**
- Weather data (**WeDar**)
- Energy asset registry (**MaStr**)
#### **Goal:**  
- Predict how much power would, wind, PV assets inject into the grid over past year on a **2×2 km grid** in germany. 
#### Question:
1. The prediction would be similar to task 1, just difference would be using **historical** weather instead of forecasts, right?
2. How do we evaluate the results? we do not have any historical feed-ins at this granularity ? 

## 3.  Possible location of storage systems based on existing high-voltage grid

#### **Inputs:**
- Hourly modeled wind/PV feed-in on a 2×2 km grid (from Tasks 1–2).
- **map of the HV grid** (nodes, lines, thermal limits).
#### **Goal:**  
Propose storage locations at nearby HV nodes/substations where charging during surplus and discharging during constraint would reduce those problems the most.
#### Question:
1. From where would we get the data of **map of the HV grid** nodes and lines

## 4.  an appropriate algorithm for the sizing of storages should be designed.

#### **Goal:**  
Create an **algorithm** that decides **how big** each storage unit should be at a chosen grid location
#### Question:
1. What do you mean by size? - how fast the storage can charge/discharge (handles spikes) or how much energy it can store?
2. We would need power line data between HV grid and consumer transformers

## 5.  Literature Review
## 6. A basic GUI to show all analysis
## 7.  Prediction of inter-area power flow for different weather scenarios
==NOTE: More explaination needed for this task==

**Input:**
- Hourly modeled wind/PV feed-in on the **2×2 km** grid (from Tasks 1–2), aggregated to chosen **areas** (quadrants or TSO zones).
- **Weather scenarios** (e.g., high-wind, low-wind, high-solar, typical) using WeDar.
- **HV grid model** (nodes, lines, limits) covering those areas
  
**Goal:**  
Predict the **direction and magnitude of power flows between areas** under each weather scenario

#### Question:
1. Same question as above, From where would we get the data of **map of the HV grid** nodes and lines
# General Questions 
1. What does "but is not limited to" mean? What can be added to scope, and will it be added only if the described tasks in the thesis are completed before time and we have time to finish the extra tasks? will it be added with mutual agreement? or it can be added anytime and would be cumpolsory to complete?
2. How much amount of technical knowledge would be needed? If needed, would it be possible to share the reading materials which can help me understand basics to do this thesis?


Mail snapshot:

I have added my comments in red. Feel free to ask again if my answers did not suffice.   
  
Regards,  
Mirco

**Von:** Het Viren Parekh <[het.parekh@tuhh.de](mailto:het.parekh@tuhh.de)>   
**Gesendet:** Dienstag, 21. Oktober 2025 11:30  
**An:** Mirco Woidelko, M.Sc., M.A. <[mirco.fabian.woidelko@tuhh.de](mailto:mirco.fabian.woidelko@tuhh.de)>  
**Betreff:** Questions on thesis scope description

Dear Mirco,  
  
I just went through the thesis description you provided and I have a few questions. I’ll start with the forecast task and then go item by item.  
  
In _“the infeed of renewable power based on weather forecast”_, my understanding is that we predict how much power would, wind, PV assets inject into the grid over time on a **2×2 km quadrant** in Germany on **hourly** basis for **0–48 hours ahead** right? Right!  
**Questions:**  
1. Is hourly time step and 0–48 h horizon, OK? YES  
2. Is there any **official 2×2 km quadrant definition** or I can create one by myself? That is predefined by the weather forecast model used.  
3. Could you clarify what **“including confidence level (p-value, etc.)”** mean (mentioned in pdf)? from what I remember from our meeting is after the prediction when the time arrives, we evaluate the accuracy by comparing the predicted with ground truth, Is that what you mean? Confidence level refers to the accuracy, in other words the interval ([https://en.wikipedia.org/wiki/Confidence_interval](https://en.wikipedia.org/wiki/Confidence_interval)). There is an array of predictions, which can be used to create the interval and this interval is than used to determine the confidence with regard to the actual measured weather and resulting power.   
4. Do we have any data for **actual current and historical feed-in**, with **geographic granularity** of quadrant? If not, how are we suppose to do question 3? Yes, there is data available from SMARD, which is not as accurate as we would like, but at least with the aggregated feed-in for certain sectors it can be compared. By the way, potential feed-in is not actual feed-in. It is common that wind mills or PVs are disconnected from the grid if there is grid bottleneck.

  
In _“Spatial infeed distribution across Germany over the year”_, my understanding is that we Predict how much power would, wind, PV assets inject into the grid over past year on a **2×2 km grid** in Germany, right? This extends upon before, so basically this is step two ( first do it for one single 2 by 2 square and maybe one 100h interval , then extend it to Germany)   
**Question:**  
1. The prediction would be similar to task 1, just difference would be using **historical** weather (maybe for 2023 or 2024) instead of **forecasts**, right?  
2. How do we evaluate the results? we do not have any historical feed-ins at quadrant level? 

  
  
In _“Possible location of storage systems based on existing high-voltage grid task”_  
**Questions:**  
1. from where would we get the data of the HV grid nodes and lines at granularity of latitude and longitude? That is all given in the grid map of entso-e, we can make that available/ send you the link to the website.   
  
In _“an appropriate algorithm for the sizing of storages should be designed”_   
**Questions:**  
2. What do you mean by size? - how much energy it can store? Sizing here refers to the power output, but energy storage capacity could be include, which I think would be to much for the thesis.   
3. We would need power line data between HV grid and consumer transformers, where can we get that from? No, we would not. This is purely on coupling between different HV grids ( Control zone level). The data for consumption overall could be made available by SMARD or Fraunhofer database.  
  
In _“Prediction of inter-area power flow for different weather scenarios”_  
**Questions:**  
4. do we have data of for line topology and consumer/load grids? As mentioned before.   
  
In _“General”_  
5. What does **“but is not limited to”** mean? any additions are **optional?** and will it be added **only by mutual agreement** (ideally after core tasks are completed), since the core tasks are already quite complicated which can occupy the maximum amout of duration. We thing the assigned task are complicated enough, too. Yet, if you find something that can also be found from the data or different aspects that we might not have seen, you may add them.   
6. How much amount of technical knowledge would be needed? If needed, would it be possible to share the reading materials which can help me understand basics required for the thesis? That could be made available as soon as you need them. I think it is best, if instead of doing much reading you make use of our individual expertise ( you have three people you can ask). This way, you get a quick answer and if there is a bigger issue, any of us supervisors can deep-dive into it and make the information quickly available. We do not expect you to be an expert in electrical engineering, but our expert for data fusion and analysis.   
  
In _“WeDar”_  
From first glance, it looks like the data is for single location point?, do we have a complete data (with whether data for all lat and lon points of germany) for each day with time information, if yes, then no question. YES  
  
Best Regards,  
Het Parekh