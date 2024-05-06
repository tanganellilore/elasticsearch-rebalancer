from setuptools import setup

REQUIREMENTS = (
    'click>=8.1.7',
    'humanize>=4.9.0',
    'requests>=2.31.0',
    'elasticsearch>=8.13.0',
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
