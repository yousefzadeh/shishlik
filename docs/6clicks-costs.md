Here's a breakdown of how our current Azure spend looks like in terms of what is relevant to Yellowfin:

* By January we will have 6clicks instances hosted in 11 different regions, so multiple the below by 11\.  
* **Operational database:**  
  Azure SQL Database Hyperscale  
  Capacity: 2 to 6 cores, with or without read replicas, depending on number of users/customers in the region  
  Cost before reservation: USD$300 \- $2000 / month depending on number of cores  
  Cost after 3 yr reservation: USD$150 \- $850 / month  
* **Yellowfin database**  
  **(This doesn't drive the reports, just Yellowfin's own operations)**  
  Azure SQL Database Hyperscale  
  Capacity: 2 cores

**\* Synapse**  
We found the managed service was too expensive so moved to a single virtual machine with 4 CPUs at $280 / month.

\* **Yellowfin license**  
Approx AUD $60,000 / year covering all regions. This is significantly discounted on their normal ask.

**Budget going ahead**

I'm relatively open to ideas, but as a starting point for what would be *comfortable*, here are some figures:

3 larger regions (AU, US, UK): \+ USD$1000 / month each

3 mid-size regions (AU Gov, UAE, DE): \+ USD$500 / month each.

5 smaller regions (JP, SG, CA, QA, US Gov): \+ USD$250 / month each.