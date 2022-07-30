import { useState } from "react";

const AutoComplete = ({ data }) => {
    const [suggestions, setSuggestions] = useState([]);
    const [suggestionIndex, setSuggestionIndex] = useState(0);
    const [suggestionsActive, setSuggestionsActive] = useState(false);
    const [value, setValue] = useState("");

    const handleChange = (e) => {
        const query = e.target.value.toLowerCase();
        setValue(e.target.value);
        if (query.length > 0) {
            let completions = [];
            fetch(`http://localhost:8080/queries?prefix=${query}`)
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
                  {suggestion}
                </li>
              );
            })}
          </ul>
        );
      };

      return (
        <form className="autocomplete" onSubmit={(e) => {
            e.preventDefault();
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
