import { useState } from "react";

const DEMO_PORT = process.env.BACKEND_PORT || 8080;
const DEMO_HOST = "localhost";

const AutoComplete = () => {
    const [suggestions, setSuggestions] = useState([]);
    const [suggestionIndex, setSuggestionIndex] = useState(0);
    const [suggestionsActive, setSuggestionsActive] = useState(false);
    const [value, setValue] = useState("");

    const normalizeQuery = (query) => query.toLowerCase();
    const handleChange = (e) => {
        const query = normalizeQuery(e.target.value);
        setValue(e.target.value);
        if (query.length > 0) {
            let completions = [];
            fetch(`http://${DEMO_HOST}:${DEMO_PORT}/queries?prefix=${query}`)
            .then(
                 (response) => response.json()
                )
                .then(function (data) {
                    for (let i = data.queries.length - 1; i >= 0; --i) {
                      completions.push(data.queries[i].suffix);
                    }
                    setSuggestions(completions);
                    setSuggestionsActive(true);
                    return true;
                  }).catch(function (err) {
                    console.warn('Something went wrong.', err);
                    return false;
                  });
        } else {
          setSuggestionsActive(false);
        }
      };

      const handleClick = (e) => {
        setSuggestions([]);
        setValue(e.target.innerText);
        setSuggestionsActive(false);
      };

      const handleKeyDown = (e) => {
        // UP ARROW
        if (e.keyCode === 38) {
          if (suggestionIndex === 0) {
            return;
          }
          setSuggestionIndex(suggestionIndex - 1);
        }
        // DOWN ARROW
        else if (e.keyCode === 40) {
          if (suggestionIndex - 1 === suggestions.length) {
            return;
          }
          setSuggestionIndex(suggestionIndex + 1);
        }
        // TAB
        else if (e.keyCode === 9) {
            if (suggestionsActive) {
              e.preventDefault();
              setValue(suggestions[suggestionIndex]);
              setSuggestionIndex(0);
              setSuggestionsActive(false);
            }
        }
      };

      const Suggestions = () => {
        return (
          <ul className="suggestions">
            {suggestions.map((suggestion, index) => {
              return (
                <li
                  className={index === suggestionIndex ? "active" : ""}
                  key={index}
                  onClick={handleClick}
                >
                  {<b>{value}</b>}{suggestion.substring(value.length)}
                </li>
              );
            })}
          </ul>
        );
      };

      const addQuery = (query) => {
        console.log("Query: " + query);
        fetch(`http://${DEMO_HOST}:${DEMO_PORT}/add_query`, {
          method: 'POST',
          headers: {
            Accept: 'application.json',
            'Content-Type': 'text/plain;charset=utf-8'
          },
          body: normalizeQuery(query),
        })
      }

      return (
        <form className="autocomplete" autoComplete="off" onSubmit={(e) => {
            console.log(value);
            e.preventDefault();
            addQuery(value);
            return false;
          }}>
          <input
            type="text"
            placeholder="Type query"
            value={value}
            onChange={handleChange}
            onKeyDown={handleKeyDown}
          />
          {suggestionsActive && <Suggestions />}
        </form>
      );
};

export default AutoComplete;
