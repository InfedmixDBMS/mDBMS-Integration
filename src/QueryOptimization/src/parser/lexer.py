"""
Lexer Module - Tokenizes SQL query strings
"""

import re
KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR",
    "ORDER", "BY", "INNER", "LEFT", "RIGHT", "OUTER", "AS",
    "GROUP", "HAVING"
}

class Lexer:
    def tokenize(self, query: str):
        """
        Tokenize a SQL query string into tokens
        """
        query = query.strip()
        pattern = (r"'[^']*'|\"[^\"]*\"|[A-Za-z_][A-Za-z0-9_.]*|"
                r"<=|>=|<>|!=|=|<|>|\*|,|\(|\)|\d+")
        tokens = re.findall(pattern, query)
        return [t.upper() if t.upper() in KEYWORDS else t for t in tokens]