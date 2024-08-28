from dataclasses import dataclass

@dataclass
class DremioLexiconWords:
    ALTER: str = 'ALTER'
    ASSIGN: str = 'ASSIGN'
    AT: str = 'AT'
    BRANCH: str = 'BRANCH'
    CREATE: str = 'CREATE'
    DROP: str = 'DROP'
    MAIN: str = 'MAIN'
    SHOW: str = 'SHOW'
    SPACE_CHAR: str = ' '
    VIEW: str = 'VIEW'


@dataclass
class DremioLexiconPhrases:
    AT_BRANCH: str = ' AT BRANCH '
    AT_COMMIT: str = ' AT COMMIT '
    CREATE_OR_REPLACE: str = 'CREATE OR REPLACE '
    INTO: str = ' INTO '
    IN: str = ' IN '
    IN_CATALOG = ' IN CATALOG '
    MERGE_BRANCH: str = 'MERGE BRANCH '
    SHOW_CREATE_VIEW: str = 'SHOW CREATE VIEW '
