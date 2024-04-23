from setuptools import setup

REQUIREMENTS = (
    'click',
    'humanize',
    'requests',
)


if __name__ == '__main__':
    setup(
        name='elasticsearch-rebalancer',
        description='Pokes Elasticsearch to balance itself sensibly.',
        version='1.0',
        author='Lorenzo Tanganelli',
        author_email='tbd',
        packages=[
            'elasticsearch_rebalancer',
        ],
        url='https://github.com/EDITD/elasticsearch-rebalancer',
        python_requires='>=3.6',
        install_requires=REQUIREMENTS,
        entry_points={
            'console_scripts': (
                'es-rebalance=elasticsearch_rebalancer.rebalance:rebalance_elasticsearch',
            ),
        },
    )
