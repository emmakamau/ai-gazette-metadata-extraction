# db_operations.py
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import UUID
import uuid

import os

load_dotenv()
db_password = os.getenv('DB_PASSWORD')
Base = declarative_base()


class FullText(Base):
    __tablename__ = 'full_texts'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    content = Column(Text)
    name = Column(Text)
    section_texts = relationship("SectionText", back_populates="full_text")


class SectionText(Base):
    __tablename__ = 'section_texts'
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    full_text_id = Column(UUID(as_uuid=True), ForeignKey('full_texts.id'))
    section_content = Column(Text)
    filename = Column(String(255))
    page_number = Column(Integer)
    gazette_notice_number = Column(String(255))
    name_of_holder = Column(ARRAY(String))
    registration_number = Column(ARRAY(String))
    location = Column(String(255))
    full_text = relationship("FullText", back_populates="section_texts")


class DatabaseOperations:
    def __init__(self, database_url):
        self.engine = create_engine(database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def insert_full_text(self, doc_id, content, name):
        session = self.Session()
        full_text = FullText(id=doc_id, content=content, name=name)
        session.add(full_text)
        session.commit()
        session.refresh(full_text)
        session.close()
        return full_text.id

    def insert_section_text(self, full_text_id, section_content, filename, page_number, gazette_notice_number,
                            name_of_holder, registration_number, location):
        session = self.Session()
        section_text = SectionText(
            full_text_id=full_text_id,
            section_content=section_content,
            filename=filename,
            page_number=page_number,
            gazette_notice_number=gazette_notice_number,
            name_of_holder=name_of_holder,
            registration_number=registration_number,
            location=location
        )
        session.add(section_text)
        session.commit()
        session.close()

    def get_full_text_by_id(self, doc_id):
        session = self.Session()
        full_text = session.query(FullText).filter(FullText.id == doc_id).first()
        session.close()
        return full_text

    def get_sections_by_doc_id(self, doc_id):
        session = self.Session()
        sections = session.query(SectionText).filter(SectionText.full_text_id == doc_id).all()
        session.close()
        return sections


