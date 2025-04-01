def with_class_setup_cleanup(setup_func, cleanup_func):
    """
    Class decorator that runs setup_func once before any test methods in the class,
    and cleanup_func once after all tests have executed.
    """

    def decorator(cls):
        original_setup_class = getattr(cls, "setup_class", None)
        original_teardown_class = getattr(cls, "teardown_class", None)

        @classmethod
        def new_setup_class(cls_):
            print(f"[{cls_.__name__}] Running class-level setup...")
            setup_func(cls_)
            if original_setup_class:
                original_setup_class()
            print(f"[{cls_.__name__}] Class-level setup complete.")

        @classmethod
        def new_teardown_class(cls_):
            if original_teardown_class:
                original_teardown_class()
            print(f"[{cls_.__name__}] Running class-level cleanup...")
            cleanup_func(cls_)
            print(f"[{cls_.__name__}] Class-level cleanup complete.")

        cls.setup_class = new_setup_class
        cls.teardown_class = new_teardown_class
        return cls

    return decorator
