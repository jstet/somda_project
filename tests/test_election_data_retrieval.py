from somda_project.pipelines import get_election_data


def test_get_election_data():
    result = get_election_data()
    print(result)  # Add this line to check the structure and values of the result dictionary
    assert isinstance(result, dict)
    assert "CZ" in result
    assert result["CZ"][2009]["turnout"] == 28.22
    assert result["CZ"][2014]["turnout"] == 18.2
    assert result["CZ"][2019]["turnout"] == 28.72


4
