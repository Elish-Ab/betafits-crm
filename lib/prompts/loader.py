"""
Prompt loader and manager for LLM prompt templates.

This module handles loading, caching, and rendering YAML prompt templates
with support for Jinja2 variable substitution and Pydantic validation.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

# Prompt file directory
PROMPTS_DIR = Path(__file__).parent
PROMPT_FILES = {
    "classifier": "classifier_prompt.yaml",
    "entity_extractor": "entity_extractor_prompt.yaml",
    "relation_extractor": "relation_extractor_prompt.yaml",
    "response_drafter": "response_drafter_prompt.yaml",
}


class PromptTemplate:
    """Represents a loaded prompt template with system, instruction, and examples."""

    def __init__(
        self,
        name: str,
        system_prompt: str,
        instruction_template: str,
        few_shot_examples: Optional[list[dict[str, Any]]] = None,
        schema: Optional[dict[str, Any]] = None,
        post_processing: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize a prompt template.

        Args:
            name: Template name (e.g., "classifier")
            system_prompt: System-level instructions for the LLM
            instruction_template: Template with {variable} placeholders
            few_shot_examples: List of example input/output pairs
            schema: JSON schema for validation
            post_processing: Post-processing rules
        """
        self.name = name
        self.system_prompt = system_prompt
        self.instruction_template = instruction_template
        self.few_shot_examples = few_shot_examples or []
        self.schema = schema
        self.post_processing = post_processing or {}

    def render(self, **kwargs: Any) -> str:
        """
        Render the instruction template with provided variables.

        Args:
            **kwargs: Variables to substitute in template

        Returns:
            Rendered instruction string
        """
        return self.instruction_template.format(**kwargs)

    def to_dict(self) -> dict[str, Any]:
        """Convert template to dictionary representation."""
        return {
            "name": self.name,
            "system_prompt": self.system_prompt,
            "instruction_template": self.instruction_template,
            "few_shot_examples": self.few_shot_examples,
            "schema": self.schema,
            "post_processing": self.post_processing,
        }


class PromptLoader:
    """Singleton loader for prompt templates with in-memory caching."""

    _instance: Optional["PromptLoader"] = None
    _templates: dict[str, PromptTemplate] = {}

    def __new__(cls) -> "PromptLoader":
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def load_all(cls) -> "PromptLoader":
        """
        Load all prompt templates from disk.

        Returns:
            PromptLoader singleton instance
        """
        instance = cls()
        if not instance._templates:
            for name, filename in PROMPT_FILES.items():
                instance._load_template(name, filename)
        return instance

    @classmethod
    def get(cls, name: str) -> PromptTemplate:
        """
        Retrieve a prompt template by name.

        Args:
            name: Template name (e.g., "classifier")

        Returns:
            PromptTemplate instance

        Raises:
            KeyError: If template not found
            RuntimeError: If templates not loaded
        """
        instance = cls()
        if not instance._templates:
            instance.load_all()
        if name not in instance._templates:
            raise KeyError(
                f"Prompt template '{name}' not found. Available: {list(instance._templates.keys())}"
            )
        return instance._templates[name]

    @classmethod
    def _load_template(cls, name: str, filename: str) -> None:
        """
        Load a single prompt template from YAML file.

        Args:
            name: Template name
            filename: YAML filename
        """
        filepath = PROMPTS_DIR / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Prompt file not found: {filepath}")

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f)

            if not isinstance(content, dict):
                raise ValueError(f"Invalid YAML structure in {filename}: expected dict")

            system_prompt = content.get("system_prompt", "")
            instruction_template = content.get("instruction_template", "")

            if not system_prompt or not instruction_template:
                raise ValueError(
                    f"Missing required fields in {filename}: system_prompt, instruction_template"
                )

            template = PromptTemplate(
                name=name,
                system_prompt=system_prompt,
                instruction_template=instruction_template,
                few_shot_examples=content.get("few_shot_examples"),
                schema=content.get("schema"),
                post_processing=content.get("post_processing"),
            )

            instance = cls()
            instance._templates[name] = template
            logger.info(f"Loaded prompt template: {name} from {filename}")

        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse YAML in {filename}: {e}") from e

    @classmethod
    def list_templates(cls) -> list[str]:
        """
        List all available template names.

        Returns:
            List of template names
        """
        instance = cls()
        if not instance._templates:
            instance.load_all()
        return list(instance._templates.keys())

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the in-memory template cache (for testing)."""
        instance = cls()
        instance._templates.clear()


def get_classifier_prompt(**kwargs: Any) -> tuple[str, str]:
    """
    Get classifier prompt (system + rendered instruction).

    Args:
        **kwargs: Variables for template rendering

    Returns:
        Tuple of (system_prompt, instruction)
    """
    template = PromptLoader.get("classifier")
    instruction = template.render(**kwargs)
    return template.system_prompt, instruction


def get_entity_extractor_prompt(**kwargs: Any) -> tuple[str, str]:
    """
    Get entity extractor prompt (system + rendered instruction).

    Args:
        **kwargs: Variables for template rendering

    Returns:
        Tuple of (system_prompt, instruction)
    """
    template = PromptLoader.get("entity_extractor")
    instruction = template.render(**kwargs)
    return template.system_prompt, instruction


def get_relation_extractor_prompt(**kwargs: Any) -> tuple[str, str]:
    """
    Get relation extractor prompt (system + rendered instruction).

    Args:
        **kwargs: Variables for template rendering

    Returns:
        Tuple of (system_prompt, instruction)
    """
    template = PromptLoader.get("relation_extractor")
    instruction = template.render(**kwargs)
    return template.system_prompt, instruction


def get_response_drafter_prompt(**kwargs: Any) -> tuple[str, str]:
    """
    Get response drafter prompt (system + rendered instruction).

    Args:
        **kwargs: Variables for template rendering

    Returns:
        Tuple of (system_prompt, instruction)
    """
    template = PromptLoader.get("response_drafter")
    instruction = template.render(**kwargs)
    return template.system_prompt, instruction
