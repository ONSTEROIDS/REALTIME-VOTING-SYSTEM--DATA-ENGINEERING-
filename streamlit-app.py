import pandas as pd
import streamlit as st
import time


# Function to update data displayed on the dashboard
def update_data():
    # Placeholder to display last refresh time
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch voting statistics
    voters_count, candidates_count = fetch_voting_stats()

    # Display total voters and candidates metrics
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    # Fetch data from Kafka on aggregated votes per candidate
    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)

    # Debugging: print fetched data and columns
    st.write("Fetched data from Kafka:", data)
    st.write("Results columns:", results.columns)

    # Check if 'candidate_id' exists and handle missing column
    if 'candidate_id' in results.columns:
        results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    else:
        st.warning("candidate_id column is missing in the data.")

    # Debugging: Check if 'total_votes' column exists
    if 'total_votes' not in results.columns:
        st.warning("total_votes column is missing in the data.")
        st.write("Available columns:", results.columns)
    else:
        # Identify the leading candidate if 'total_votes' exists
        leading_candidate = results.loc[results['total_votes'].idxmax()]

        # Display leading candidate information
        st.markdown("""---""")
        st.header('Leading Candidate')
        col1, col2 = st.columns(2)
        with col1:
            st.image(leading_candidate['photo_url'], width=200)
        with col2:
            st.header(leading_candidate['candidate_name'])
            st.subheader(leading_candidate['party_affiliation'])
            st.subheader("Total Vote: {}".format(leading_candidate['total_votes']))

    # Display statistics and visualizations
    st.markdown("""---""")
    st.header('Statistics')
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)
    col1, col2 = st.columns(2)

    # Display bar chart and donut chart
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)

    with col2:
        donut_fig = plot_donut_chart(results, title='Vote Distribution')
        st.pyplot(donut_fig)

    # Display table with candidate statistics
    st.table(results)

    # Fetch data from Kafka on aggregated turnout by location
    location_consumer = create_kafka_consumer("aggregated_turnout_by_location")
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)

    # Debugging: print fetched location data and columns
    st.write("Fetched location data from Kafka:", location_data)
    st.write("Location result columns:", location_result.columns)

    # Handle missing 'state' column
    if 'state' in location_result.columns and 'count' in location_result.columns:
        location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
        location_result = location_result.reset_index(drop=True)
    else:
        st.warning("'state' or 'count' column is missing in the location data.")
        st.write("Available columns:", location_result.columns)

    # Display location-based voter information with pagination
    st.header("Location of Voters")
    paginate_table(location_result)

    # Update the last refresh time
    st.session_state['last_update'] = time.time()

# Function to simulate fetching voting statistics
def fetch_voting_stats():
    # You can replace this with actual data fetching logic
    return 100000, 500  # Example: 100000 voters, 500 candidates

# Function to simulate fetching data from Kafka
def fetch_data_from_kafka(consumer):
    # Simulating fetched data. Replace with actual data fetching logic
    return [
        {'candidate_id': 1, 'candidate_name': 'Alice', 'party_affiliation': 'Party A', 'total_votes': 5000, 'photo_url': 'http://example.com/alice.jpg'},
        {'candidate_id': 2, 'candidate_name': 'Bob', 'party_affiliation': 'Party B', 'total_votes': 4500, 'photo_url': 'http://example.com/bob.jpg'},
        {'candidate_id': 3, 'candidate_name': 'Charlie', 'party_affiliation': 'Party C', 'total_votes': 6000, 'photo_url': 'http://example.com/charlie.jpg'},
    ]

# Simulate Kafka consumer creation (use actual code to create Kafka consumer)
def create_kafka_consumer(topic):
    return None  # Replace with actual consumer creation logic

# Function to plot a bar chart (replace with actual plotting function)
def plot_colored_bar_chart(results):
    # Placeholder bar chart code
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.bar(results['candidate_name'], results['total_votes'], color='skyblue')
    return fig

# Function to plot a donut chart (replace with actual plotting function)
def plot_donut_chart(results, title):
    # Placeholder donut chart code
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.pie(results['total_votes'], labels=results['candidate_name'], startangle=90, wedgeprops={'width': 0.3})
    ax.set_title(title)
    return fig

# Function to paginate table (replace with actual pagination code)
def paginate_table(df):
    # Placeholder pagination logic
    st.dataframe(df)

# Run the Streamlit app
if __name__ == "__main__":
    update_data()
