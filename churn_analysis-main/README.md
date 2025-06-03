# Churn Analysis
## Developing Predictive Models to Identify At-Risk Customers and Reduce Churn Rate
![Churn_analysis_main_project%](https://github.com/t17sk18/churn_analysis/assets/46694097/81eda222-a0cd-4260-8b7b-fdb7cf81592b)
![Churn_analysis_main_project](https://github.com/t17sk18/churn_analysis/assets/46694097/7ce2d74f-d902-469d-b7cb-9dd5a7420daf)

## Analysis Objectives:

  Identifying Key Churn Drivers:

- Determine which factors contribute most significantly to customer churn within the banking and telecom industries
- Analyze the correlation between different customer attributes and the likelihood of churn.
- Identify patterns or trends that distinguish churned customers from those who remain active.

Segmentation of Customer Base:

- Segment customers based on their demographic characteristics, transactional behaviour, and activity status.
- Explore distinct customer segments and their respective churn rates.
- Identify high-value segments that are more prone to churn and those that are more loyal.

Predictive Modeling for Churn Prediction:

- Develop predictive models using machine learning algorithms to forecast which customers will likely churn in the future.
- Evaluate the performance of various models and select the most accurate and reliable one for deployment.
- Assess the predictive power of different features and their impact on model performance.

Customer Lifetime Value (CLV) Analysis:

- Calculate the CLV for different customer segments to understand the revenue potential associated with each group.
- Determine how churn rates affect the CLV of different customer segments.
- Explore strategies to maximize CLV while minimizing churn.

Retention Strategy Recommendations:

- Generate actionable insights and recommendations to improve customer retention based on analysis findings.
- Develop targeted retention campaigns tailored to customer segments and their churn risk levels.
- Evaluate the potential effectiveness of retention initiatives through simulations or A/B testing.

Continuous Monitoring and Iterative Improvement:

- Establish mechanisms for ongoing monitoring of churn rates and customer behaviour patterns.
- Implement feedback loops to continuously refine predictive models and retention strategies based on real-time data.
- Track the impact of implemented strategies on reducing churn and increasing customer loyalty over time.

Note: By focusing on these analysis objectives, the project aims to empower businesses in the banking and telecom sectors to proactively address customer churn, optimize retention efforts, and ultimately enhance overall customer satisfaction and profitability.

## DAX Functions Implementation

Active Members

ActiveMembers = CALCULATE(COUNT(Bank_Churn[CustomerId]), 'Active customers'[ActiveCategory] = "Active member")

Churn percentage

Churn% = 
var EC = [Exit Customers]
var TC = [Total Customers]
var churnper = DIVIDE(EC, TC )
RETURN churnper

Credit Card Holders

Credit Card Holders = CALCULATE(COUNT(Bank_Churn[CustomerId]), 'Credit card'[Category] = "credit card holder")

Exit Customers

Exit Customers = CALCULATE(COUNT(CustomerInfo[CustomerId]), 'Customer Exit'[ExitCategory] = "Exit")

Inactive members

Inactive members = CALCULATE(COUNT(Bank_Churn[CustomerId]), 'Active customers'[ActiveCategory] = "Inactive Member")

Non Credit Card Holders

Non credit card holders = CALCULATE(COUNT(Bank_Churn[CustomerId]), 'Credit card'[Category] = "non credit card holder")

Previous month exit customers

Previous month exit customers = CALCULATE([Exit Customers], PREVIOUSMONTH('Date Master'[Date]))

Retain Customers

Retain customers = [Total Customers] - [Exit Customers]

Total Customers

Total Customers = COUNT(Bank_Churn[CustomerId])


***************You can Include more DAX Functions as for your requirement********************

